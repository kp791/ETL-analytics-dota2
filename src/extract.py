from __future__ import annotations

import json
import os
from typing import List, Dict
from opendota import OpenDota
import logging
import time

logger = logging.getLogger(__name__)

def fetch_matches_sql(limit: int = 100) -> List[Dict]:
    """
    Fetch matches using OpenDota SQL explorer (FAST!).
    Respects 1000 row limit.
    
    Args:
        limit: Number of matches to fetch (max ~90 to stay under 1000 row limit)
        
    Returns:
        List of match dictionaries
    """
    try:
        client = OpenDota()
        
        # Each match has ~10 players, so limit rows to avoid hitting 1000 limit
        max_matches_for_query = min(limit, 90)
        row_limit = max_matches_for_query * 10
        
        # Use SQL query - removed broken date filter, use ORDER BY and LIMIT instead
        query = f"""
        SELECT 
            matches.match_id,
            matches.radiant_win,
            matches.duration,
            matches.game_mode,
            player_matches.player_slot,
            player_matches.hero_id,
            player_matches.kills,
            player_matches.deaths,
            player_matches.assists,
            player_matches.gold_per_min,
            player_matches.xp_per_min,
            player_matches.last_hits,
            player_matches.denies,
            player_matches.hero_damage,
            player_matches.tower_damage,
            player_matches.hero_healing,
            player_matches.level
        FROM matches
        JOIN player_matches USING(match_id)
        ORDER BY matches.match_id DESC
        LIMIT {row_limit}
        """
        
        logger.info(f"Executing SQL query (max {row_limit} rows for ~{max_matches_for_query} matches)...")
        rows = client.explorer(query)
        
        if not rows or len(rows) == 0:
            logger.warning(f"No matches from SQL query (got {len(rows) if rows else 0} rows), falling back to API")
            return fetch_matches_bulk(limit=limit)
        
        logger.info(f"✓ Fetched {len(rows)} player records via SQL")
        
        # Group by match_id to reconstruct match structure
        matches = {}
        for row in rows:
            match_id = row['match_id']
            
            if match_id not in matches:
                matches[match_id] = {
                    'match_id': match_id,
                    'radiant_win': row['radiant_win'],
                    'duration': row['duration'],
                    'game_mode': row['game_mode'],
                    'players': []
                }
            
            # Add player data
            matches[match_id]['players'].append({
                'player_slot': row['player_slot'],
                'hero_id': row['hero_id'],
                'kills': row['kills'],
                'deaths': row['deaths'],
                'assists': row['assists'],
                'gold_per_min': row['gold_per_min'],
                'xp_per_min': row['xp_per_min'],
                'last_hits': row['last_hits'],
                'denies': row['denies'],
                'hero_damage': row['hero_damage'],
                'tower_damage': row['tower_damage'],
                'hero_healing': row['hero_healing'],
                'level': row['level']
            })
        
        # Convert to list and limit to requested amount
        matches_list = list(matches.values())[:limit]
        logger.info(f"✓ Processed {len(matches_list)} complete matches with player data")
        
        return matches_list
        
    except Exception as e:
        logger.error(f"Error with SQL query: {e}")
        logger.warning("Falling back to API method...")
        return fetch_matches_bulk(limit=limit)

def fetch_matches_bulk(limit: int = 50) -> List[Dict]:
    """Fetch matches using pro matches API (slower fallback)."""
    try:
        client = OpenDota()
        
        logger.info("Fetching pro matches...")
        pro_matches = client.get_pro_matches()
        
        if not pro_matches:
            logger.warning("No pro matches, trying public matches")
            return fetch_matches_with_details(limit=limit)
        
        logger.info(f"Fetched {len(pro_matches)} pro match records")
        
        detailed_matches = []
        for i, match in enumerate(pro_matches[:limit]):
            match_id = match.get('match_id')
            if match_id:
                logger.info(f"Fetching details for match {i+1}/{limit}")
                try:
                    detail = client.get_match(match_id)
                    if detail and 'players' in detail and len(detail.get('players', [])) > 0:
                        detailed_matches.append(detail)
                except Exception as e:
                    logger.error(f"Error fetching match {match_id}: {e}")
                
                time.sleep(0.3)
                
                if len(detailed_matches) >= limit:
                    break
        
        logger.info(f"Successfully fetched {len(detailed_matches)} matches with full data")
        return detailed_matches
        
    except Exception as e:
        logger.error(f"Error in fetch_matches_bulk: {e}")
        raise

def fetch_public_matches(limit: int = 50) -> List[Dict]:
    """Fetch public matches from OpenDota API."""
    try:
        client = OpenDota()
        public_matches = client.get('publicMatches')
        
        if not public_matches:
            logger.warning("No public matches returned, using pro matches")
            public_matches = client.get_pro_matches()
        
        logger.info(f"Fetched {len(public_matches)} public matches")
        return public_matches[:limit]
    except Exception as e:
        logger.error(f"Error fetching matches: {e}")
        raise

def fetch_match_details(match_id: int) -> Dict:
    """Fetch detailed match data including player stats."""
    try:
        client = OpenDota()
        match = client.get_match(match_id)
        return match
    except Exception as e:
        logger.error(f"Error fetching match {match_id}: {e}")
        return None

def fetch_matches_with_details(limit: int = 20) -> List[Dict]:
    """Fetch matches and enrich with detailed player data."""
    try:
        matches = fetch_public_matches(limit=100)
        
        detailed_matches = []
        for i, match in enumerate(matches[:limit]):
            match_id = match.get('match_id')
            if match_id:
                logger.info(f"Fetching details for match {i+1}/{limit}")
                detail = fetch_match_details(match_id)
                if detail and 'players' in detail:
                    detailed_matches.append(detail)
                time.sleep(0.3)
                if len(detailed_matches) >= limit:
                    break
        
        logger.info(f"Fetched detailed data for {len(detailed_matches)} matches")
        return detailed_matches
    except Exception as e:
        logger.error(f"Error fetching matches with details: {e}")
        raise

def fetch_hero_stats() -> List[Dict]:
    """Fetch hero statistics."""
    try:
        client = OpenDota()
        hero_stats = client.get_hero_stats()
        logger.info(f"Fetched stats for {len(hero_stats)} heroes")
        return hero_stats
    except Exception as e:
        logger.error(f"Error fetching hero stats: {e}")
        raise

def save_data(data: List[Dict], directory: str, filename: str) -> str:
    """Save data to JSON file."""
    os.makedirs(directory, exist_ok=True)
    filepath = os.path.join(directory, filename)
    
    with open(filepath, 'w') as f:
        json.dump(data, f, indent=2)
    
    logger.info(f"Saved {len(data)} records to {filepath}")
    return filepath

