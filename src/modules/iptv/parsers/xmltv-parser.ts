import { promises as fs } from 'fs';
import { parseStringPromise } from 'xml2js';
import { getLogger } from '../../../utils/logger';
import { config } from '../../../utils/config';
import { parseDate } from '../utils';
import type { ChannelEntry, ProgrammeEntry } from '../../../interfaces/iptv';

const logger = getLogger();

/**
 * Represents parsed XMLTV data including both channels and programmes.
 */
export interface XMLTVData {
    channels: ChannelEntry[];
    programmes: ProgrammeEntry[];
}

/**
 * Parses an XMLTV file to extract both channel and programme entries.
 * 
 * @param {string} filePath - Path to the XMLTV file
 * @returns {Promise<XMLTVData>} - Object containing arrays of channels and programmes
 */
export async function parseXMLTVFull(filePath: string): Promise<XMLTVData> {
    const result: XMLTVData = { channels: [], programmes: [] };

    try {
        const xmlContent = await fs.readFile(filePath, 'utf8');
        logger.debug(`XMLTV file size: ${xmlContent.length} bytes`);

        const parsedXml = await parseStringPromise(xmlContent);

        // Parse channels
        if (parsedXml.tv.channel && parsedXml.tv.channel.length > 0) {
            logger.info(`Found ${parsedXml.tv.channel.length} channels in XMLTV`);
            for (const channel of parsedXml.tv.channel) {
                try {
                    const parsedChannel = parseChannelEntry(channel);
                    result.channels.push(parsedChannel);
                } catch (error) {
                    const id = channel.$?.id || 'unknown';
                    logger.error(`Error parsing channel "${id}": ${error}`);
                }
            }
        }

        // Parse programmes
        if (parsedXml.tv.programme && parsedXml.tv.programme.length > 0) {
            logger.info(`Found ${parsedXml.tv.programme.length} programmes in XMLTV`);
            for (const programme of parsedXml.tv.programme) {
                try {
                    const parsedProgramme = parseProgrammeEntry(programme);
                    result.programmes.push(parsedProgramme);
                } catch (error) {
                    const title = extractTextContent(programme.title?.[0]) || 'unknown';
                    logger.error(`Error parsing programme "${title}": ${error}`);
                }
            }
            logProgrammeStatistics(result.programmes);
        }

    } catch (error) {
        logger.error(`Error parsing XMLTV: ${error}`);
    }

    return result;
}

/**
 * Parses a single channel entry from XMLTV data.
 * 
 * @param {any} channel - Raw channel data from XMLTV
 * @returns {ChannelEntry} - Structured channel entry
 */
function parseChannelEntry(channel: any): ChannelEntry {
    const channelId = channel.$?.id || '';

    // Extract display names - XMLTV can have multiple display-name elements
    const displayNames = channel['display-name'] || [];
    let tvgName = '';
    let channelNumber = '';

    for (const displayName of displayNames) {
        const name = extractTextContent(displayName);
        // First display-name is usually the full name (e.g., "102 Doctor Who")
        if (!tvgName) {
            tvgName = name;
        }
        // Check if this is a channel number (numeric only)
        if (/^\d+$/.test(name) && !channelNumber) {
            channelNumber = name;
        }
    }

    // Extract icon URL
    const iconSrc = channel.icon?.[0]?.$?.src || '';

    // Construct stream URL from STREAM_BASE_URL if available
    // Supports patterns like: https://example.com/stream/channels/{channel}.m3u8
    // where {channel} is replaced with the channel number
    let streamUrl = '';
    if (config.STREAM_BASE_URL && channelNumber) {
        streamUrl = config.STREAM_BASE_URL.replace('{channel}', channelNumber);
        logger.debug(`Constructed stream URL for channel ${channelNumber}: ${streamUrl}`);
    }

    logger.debug(`Parsed XMLTV channel: ${channelId} -> ${tvgName}`);

    return {
        xui_id: channelNumber ? parseInt(channelNumber, 10) : 0,
        tvg_id: channelId,
        tvg_name: tvgName,
        tvg_logo: iconSrc,
        group_title: '',
        url: streamUrl,
        created_at: new Date().toISOString(),
        country: ''
    };
}

/**
 * Parses an XMLTV file to extract programme entries.
 * 
 * @param {string} filePath - Path to the XMLTV file
 * @returns {Promise<ProgrammeEntry[]>} - Array of parsed programme entries
 */
export async function parseXMLTV(filePath: string): Promise<ProgrammeEntry[]> {
    const programmes: ProgrammeEntry[] = [];
    try {
        const xmlContent = await fs.readFile(filePath, 'utf8');
        logger.debug(`XMLTV file size: ${xmlContent.length} bytes`);

        const parsedXml = await parseStringPromise(xmlContent);
        logger.info(`Found ${parsedXml.tv.programme?.length || 0} programmes in XMLTV`);

        if (!parsedXml.tv.programme || parsedXml.tv.programme.length === 0) {
            logger.error('No programmes found in XMLTV file');
            return programmes;
        }

        // Debug first programme structure
        const sampleProgramme = parsedXml.tv.programme[0];
        logger.debug(`Sample programme structure: ${JSON.stringify(sampleProgramme).substring(0, 500)}...`);

        for (const programme of parsedXml.tv.programme) {
            try {
                const parsedProgramme = parseProgrammeEntry(programme);
                programmes.push(parsedProgramme);
            } catch (error) {
                const title = extractTextContent(programme.title?.[0]) || 'unknown';
                logger.error(`Error parsing programme "${title}": ${error}`);
            }
        }

        logProgrammeStatistics(programmes);
    } catch (error) {
        logger.error(`Error parsing XMLTV: ${error}`);
    }
    return programmes;
}

/**
 * Parses a single programme entry from XMLTV data.
 * 
 * @param {any} programme - Raw programme data from XMLTV
 * @returns {ProgrammeEntry} - Structured programme entry
 * @throws {Error} - If programme is missing required fields
 */
function parseProgrammeEntry(programme: any): ProgrammeEntry {
    const title = extractTextContent(programme.title?.[0]);
    const description = extractTextContent(programme.desc?.[0]);
    const category = extractTextContent(programme.category?.[0]);
    const subtitle = extractTextContent(programme['sub-title']?.[0]);

    // Parse episode number from various formats
    const { episodeNum, season, episode } = parseEpisodeNumber(programme['episode-num']);

    // Extract icon/image URLs
    const icon = programme.icon?.[0]?.$?.src || '';
    const image = extractTextContent(programme.image?.[0]) || '';

    // Extract date and previously-shown flag
    const date = extractTextContent(programme.date?.[0]);
    const previouslyShown = programme['previously-shown'] !== undefined;

    const startStr = programme.$.start;
    const stopStr = programme.$.stop;

    if (!startStr || !stopStr) {
        throw new Error(`Programme missing start/stop times: ${title}`);
    }

    logger.debug(`Parsing programme "${title}" with start: ${startStr}, stop: ${stopStr}`);

    const start = parseDate(startStr);
    const stop = parseDate(stopStr);

    if (start.getTime() === stop.getTime()) {
        logger.warn(`Programme "${title}" has identical start and stop times: ${startStr}`);
    }

    return {
        start: start.toISOString(),
        stop: stop.toISOString(),
        start_timestamp: Math.floor(start.getTime() / 1000),
        stop_timestamp: Math.floor(stop.getTime() / 1000),
        channel: programme.$.channel,
        title,
        description,
        category,
        subtitle,
        episode_num: episodeNum,
        season,
        episode,
        icon,
        image,
        date,
        previously_shown: previouslyShown,
        created_at: new Date().toISOString(),
    };
}

/**
 * Extracts text content from an XMLTV element.
 * 
 * @param {any} element - XML element that may contain text
 * @returns {string} - Extracted text or empty string if not found
 */
function extractTextContent(element: any): string {
    if (!element) {
        return '';
    }

    if (typeof element === 'string') {
        return element;
    } else if (element._) {
        return element._;
    }

    return '';
}

/**
 * Parses episode number from XMLTV episode-num elements.
 * Supports "onscreen" format (S2E27) and "xmltv_ns" format (1.26.0/1).
 * 
 * @param {any[]} episodeNumElements - Array of episode-num elements
 * @returns {{ episodeNum: string, season: number | undefined, episode: number | undefined }}
 */
function parseEpisodeNumber(episodeNumElements: any[]): {
    episodeNum: string;
    season: number | undefined;
    episode: number | undefined;
} {
    let episodeNum = '';
    let season: number | undefined;
    let episode: number | undefined;

    if (!episodeNumElements || !Array.isArray(episodeNumElements)) {
        return { episodeNum, season, episode };
    }

    for (const epNum of episodeNumElements) {
        const system = epNum.$?.system;
        const value = extractTextContent(epNum);

        if (system === 'onscreen') {
            // Format: S2E27
            episodeNum = value;
            const onscreenMatch = value.match(/S(\d+)E(\d+)/i);
            if (onscreenMatch && onscreenMatch[1] && onscreenMatch[2]) {
                season = parseInt(onscreenMatch[1], 10);
                episode = parseInt(onscreenMatch[2], 10);
            }
        } else if (system === 'xmltv_ns' && !season) {
            // Format: season.episode.part (0-indexed)
            // e.g., "1.26.0/1" means season 2, episode 27
            const nsParts = value.split('.');
            if (nsParts.length >= 2 && nsParts[0] && nsParts[1]) {
                const seasonPart = parseInt(nsParts[0], 10);
                const episodePart = parseInt(nsParts[1], 10);
                if (!isNaN(seasonPart)) season = seasonPart + 1; // Convert to 1-indexed
                if (!isNaN(episodePart)) episode = episodePart + 1; // Convert to 1-indexed
            }
        }
    }

    return { episodeNum, season, episode };
}

/**
 * Logs statistics about the parsed programme data.
 * 
 * @param {ProgrammeEntry[]} programmes - Array of programme entries
 */
function logProgrammeStatistics(programmes: ProgrammeEntry[]): void {
    // Add summary statistics
    const channels = new Set(programmes.map(p => p.channel)).size;
    logger.info(`Parsed ${programmes.length} programmes across ${channels} channels from XMLTV file`);

    // Check for date range in the data
    if (programmes.length > 0) {
        const dates = programmes.map(p => new Date(p.start));
        const minDate = new Date(Math.min(...dates.map(d => d.getTime())));
        const maxDate = new Date(Math.max(...dates.map(d => d.getTime())));
        logger.info(`Programme date range: ${minDate.toISOString()} to ${maxDate.toISOString()}`);
    }
}
