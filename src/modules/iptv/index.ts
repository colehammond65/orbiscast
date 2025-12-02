import { promises as fs } from 'fs';
import { getLogger } from '../../utils/logger';
import { config } from '../../utils/config';
import { clearCache, getCachedFile, getCachedFilePath } from '../../utils/cache';
import { clearChannels, addChannels, clearProgrammes, addProgrammes, getChannelEntries } from '../database';
import { fetchWithRetry } from './downloaders';
import { fromPlaylistLine } from './parsers/playlist-parser';
import { parseXMLTV, parseXMLTVFull } from './parsers/xmltv-parser';
import { isProgrammeDataStale } from './utils';
import { scheduleIPTVRefresh } from './schedulers';
import type { ChannelEntry } from '../../interfaces/iptv';

const logger = getLogger();

/**
 * Downloads IPTV data, caches it, and fills the database with channels and programmes.
 * Should only be called at startup to initialize data and scheduling.
 * 
 * @param {boolean} force - Whether to force download even if cache exists
 * @returns {Promise<void>}
 */
export async function downloadCacheAndFillDb(force = false): Promise<void> {
    logger.debug('Cache download started and parsing with force: ' + force);

    // First try to get XMLTV data (which has both channels and programmes)
    await fillDbFromXMLTV(force);

    // Then try M3U playlist for additional channel info (URLs)
    await fillDbChannelsFromPlaylist(force);

    logger.debug('Finished parsing');
    await clearCache();

    // Only schedule refresh if this is initial startup, not a scheduled refresh
    if (!force) {
        scheduleIPTVRefresh();
    }
}

/**
 * Fills the database with channels and programmes from XMLTV file.
 * 
 * @param {boolean} force - Whether to force download even if cache exists
 * @returns {Promise<void>}
 */
async function fillDbFromXMLTV(force = false): Promise<void> {
    logger.debug('Starting to fill database from XMLTV');

    const isStale = await isProgrammeDataStale();

    if (!isStale && !force) {
        logger.info('XMLTV data is up to date');
        return;
    }

    logger.info('Fetching XMLTV...');
    let xmltvContent: Buffer | null = await getCachedFile('xmltv.xml');
    if (!xmltvContent || force) {
        xmltvContent = await fetchWithRetry(config.XMLTV, 'xmltv.xml');
    }

    if (!xmltvContent) {
        logger.error('No XMLTV content available');
        return;
    }

    const xmltvPath = await getCachedFilePath('xmltv.xml');
    if (!xmltvPath) {
        logger.error('XMLTV path is null. Cannot read file.');
        return;
    }

    await fs.writeFile(xmltvPath, xmltvContent);

    // Clear the buffer from memory before parsing
    xmltvContent = null;

    const xmltvData = await parseXMLTVFull(xmltvPath);

    // Add channels from XMLTV if available
    if (xmltvData.channels.length > 0) {
        logger.info(`Found ${xmltvData.channels.length} channels in XMLTV`);
        await clearChannels();
        await addChannels(xmltvData.channels);
    }

    // Add programmes
    if (xmltvData.programmes.length > 0) {
        await clearProgrammes();
        await addProgrammes(xmltvData.programmes);
    }

    // Clear parsed data references
    xmltvData.channels.length = 0;
    xmltvData.programmes.length = 0;
}

/**
 * Fills or updates channels database with data from the M3U playlist file.
 * Merges with existing XMLTV channel data by matching tvg_id.
 * 
 * @param {boolean} force - Whether to force download even if cache exists
 * @returns {Promise<void>}
 */
async function fillDbChannelsFromPlaylist(force = false): Promise<void> {
    logger.debug('Starting to fill channels from M3U playlist');

    // Skip if no playlist URL configured
    if (!config.PLAYLIST) {
        logger.debug('No playlist URL configured, skipping M3U parsing');
        return;
    }

    let playlistContent = null;
    try {
        playlistContent = await getCachedFile('playlist.m3u');
        if (playlistContent) {
            logger.debug(`Retrieved cached playlist, size: ${playlistContent.length} bytes`);
        }
    } catch (error) {
        logger.warn(`Error retrieving cached playlist: ${error}`);
    }

    if (!playlistContent || force) {
        logger.info(`${force ? 'Force flag set, downloading' : 'No cached content available'}, fetching playlist from source...`);
        try {
            playlistContent = await fetchWithRetry(config.PLAYLIST, 'playlist.m3u');
            if (playlistContent) {
                logger.debug(`Successfully downloaded playlist, size: ${playlistContent.length} bytes`);
            } else {
                logger.warn('No playlist content received');
                return;
            }
        } catch (error) {
            logger.error(`Error downloading playlist: ${error}`);
            return;
        }
    }

    if (!playlistContent) {
        return;
    }

    // Get existing channels from XMLTV
    const existingChannels = await getChannelEntries();
    const channelMap = new Map<string, ChannelEntry>();

    // Index existing channels by tvg_id
    for (const ch of existingChannels) {
        if (ch.tvg_id) {
            channelMap.set(ch.tvg_id, ch);
        }
    }

    logger.info('Parsing M3U playlist for channel URLs...');
    const playlistChannels: ChannelEntry[] = [];
    let channel: ChannelEntry | null = null;

    // Convert buffer to string and split, then clear buffer
    const lines = playlistContent.toString().split('\n');
    playlistContent = null; // Free the buffer

    for (const line of lines) {
        if (line.startsWith('#EXTINF:')) {
            channel = fromPlaylistLine(line);
        } else if (channel && !line.startsWith('#') && line.trim()) {
            channel.url = line.trim();
            channel.created_at = new Date().toISOString();
            playlistChannels.push(channel);
            channel = null;
        }
    }

    logger.info(`Parsed ${playlistChannels.length} channels from M3U playlist`);

    // Merge playlist channels with XMLTV channels
    for (const plCh of playlistChannels) {
        const existingCh = plCh.tvg_id ? channelMap.get(plCh.tvg_id) : null;
        if (existingCh) {
            // Update existing channel with URL from playlist
            existingCh.url = plCh.url;
            if (plCh.group_title) existingCh.group_title = plCh.group_title;
            if (plCh.country) existingCh.country = plCh.country;
        } else {
            // Add new channel from playlist
            channelMap.set(plCh.tvg_id || plCh.tvg_name || `playlist_${playlistChannels.indexOf(plCh)}`, plCh);
        }
    }

    // Save merged channels
    const mergedChannels = Array.from(channelMap.values());
    if (mergedChannels.length > 0) {
        await clearChannels();
        await addChannels(mergedChannels);
        logger.info(`Saved ${mergedChannels.length} merged channels to database`);
    }
}

/**
 * Clears and fills the channels database with data from the playlist file.
 * 
 * @param {boolean} force - Whether to force download even if cache exists
 * @returns {Promise<void>}
 * @deprecated Use fillDbChannelsFromPlaylist instead
 */
export async function fillDbChannels(force = true): Promise<void> {
    logger.debug('Starting to fill the channels database');

    await clearChannels();
    logger.info('Fetching playlist...');

    let playlistContent = null;
    try {
        playlistContent = await getCachedFile('playlist.m3u');
        if (playlistContent) {
            logger.debug(`Retrieved cached playlist, size: ${playlistContent.length} bytes`);
        }
    } catch (error) {
        logger.warn(`Error retrieving cached playlist: ${error}`);
    }

    if (!playlistContent || force) {
        logger.info(`${force ? 'Force flag set, downloading' : 'No cached content available'}, fetching from source...`);
        try {
            playlistContent = await fetchWithRetry(config.PLAYLIST, 'playlist.m3u');
            if (playlistContent) {
                logger.debug(`Successfully downloaded playlist, size: ${playlistContent.length} bytes`);
            } else {
                logger.error('Failed to download playlist: empty response');
            }
        } catch (error) {
            logger.error(`Error downloading playlist: ${error}`);
        }
    }

    if (playlistContent) {
        logger.info('Adding channels to database...');
        const channels: ChannelEntry[] = [];
        let channel: ChannelEntry | null = null;

        // Convert and clear buffer
        const lines = playlistContent.toString().split('\n');
        playlistContent = null;

        for (const line of lines) {
            if (line.startsWith('#EXTINF:')) {
                channel = fromPlaylistLine(line);
            } else if (channel && !line.startsWith('#') && line.trim()) {
                channel.url = line.trim();
                channel.created_at = new Date().toISOString();
                channels.push(channel);
                channel = null;
            }
        }
        await addChannels(channels);
    } else {
        logger.error('Failed to fetch playlist content from both cache and source');
    }
}

/**
 * Clears and fills the programme database with data from the XMLTV file.
 * Only refreshes if data is stale or forced.
 * 
 * @param {boolean} force - Whether to force download even if cache exists
 * @returns {Promise<void>}
 */
export async function fillDbProgrammes(force = false): Promise<void> {
    logger.debug('Starting to fill the programmes database');

    const isStale = await isProgrammeDataStale();

    if (isStale || force) {
        await clearProgrammes();
        logger.info('Fetching XMLTV...');

        let xmltvContent = await getCachedFile('xmltv.xml');
        if (!xmltvContent || force) {
            xmltvContent = await fetchWithRetry(config.XMLTV, 'xmltv.xml');
        }

        if (xmltvContent) {
            logger.info('Adding programmes to database...');
            const xmltvPath = await getCachedFilePath('xmltv.xml');
            if (xmltvPath) {
                await fs.writeFile(xmltvPath, xmltvContent);
                // Clear buffer before parsing
                xmltvContent = null;
                const programmes = await parseXMLTV(xmltvPath);
                await addProgrammes(programmes);
            } else {
                logger.error('XMLTV path is null. Cannot read file.');
            }
        } else {
            logger.error('No XMLTV content available. Cannot process.');
        }
    } else {
        logger.info('TV Schedule up to date');
    }
}

export { scheduleIPTVRefresh, stopIPTVRefresh } from './schedulers';
