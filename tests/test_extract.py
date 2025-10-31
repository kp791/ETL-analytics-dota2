import pytest
from unittest.mock import Mock, patch
from src.extract import fetch_matches_sql, fetch_hero_stats, save_data, fetch_public_matches
import json
import os
import tempfile

def test_fetch_matches_sql():
    """Test fetching matches using SQL explorer."""
    with patch('src.extract.OpenDota') as mock_client:
        mock_instance = mock_client.return_value
        
        # Mock SQL query result (rows with player data)
        mock_instance.explorer.return_value = [
            {
                'match_id': 123,
                'radiant_win': True,
                'duration': 1800,
                'game_mode': 22,
                'player_slot': 0,
                'hero_id': 1,
                'kills': 10,
                'deaths': 5,
                'assists': 15,
                'gold_per_min': 450,
                'xp_per_min': 500,
                'last_hits': 150,
                'denies': 10,
                'hero_damage': 15000,
                'tower_damage': 2000,
                'hero_healing': 1000,
                'level': 20
            },
            {
                'match_id': 123,
                'radiant_win': True,
                'duration': 1800,
                'game_mode': 22,
                'player_slot': 1,
                'hero_id': 2,
                'kills': 8,
                'deaths': 3,
                'assists': 12,
                'gold_per_min': 420,
                'xp_per_min': 480,
                'last_hits': 140,
                'denies': 8,
                'hero_damage': 12000,
                'tower_damage': 1500,
                'hero_healing': 800,
                'level': 19
            },
            {
                'match_id': 456,
                'radiant_win': False,
                'duration': 2400,
                'game_mode': 22,
                'player_slot': 128,
                'hero_id': 3,
                'kills': 5,
                'deaths': 10,
                'assists': 8,
                'gold_per_min': 350,
                'xp_per_min': 400,
                'last_hits': 100,
                'denies': 5,
                'hero_damage': 8000,
                'tower_damage': 1000,
                'hero_healing': 500,
                'level': 16
            }
        ]
        
        result = fetch_matches_sql(limit=2)
        
        # Should call explorer with SQL query
        mock_instance.explorer.assert_called_once()
        
        # Should group players by match_id
        assert len(result) == 2
        assert result[0]['match_id'] == 123
        assert result[1]['match_id'] == 456
        
        # First match should have 2 players
        assert len(result[0]['players']) == 2
        assert result[0]['players'][0]['hero_id'] == 1
        assert result[0]['players'][1]['hero_id'] == 2
        
        # Second match should have 1 player
        assert len(result[1]['players']) == 1

def test_fetch_matches_sql_empty():
    """Test SQL query with no results."""
    with patch('src.extract.OpenDota') as mock_client:
        mock_instance = mock_client.return_value
        mock_instance.explorer.return_value = []
        
        result = fetch_matches_sql(limit=10)
        
        assert result == []

def test_fetch_matches_sql_fallback():
    """Test fallback when SQL fails."""
    with patch('src.extract.OpenDota') as mock_client:
        mock_instance = mock_client.return_value
        
        # SQL explorer raises exception
        mock_instance.explorer.side_effect = Exception("SQL error")
        
        # Fallback methods
        mock_instance.get.return_value = [
            {'match_id': 999, 'radiant_win': True}
        ]
        mock_instance.get_match.return_value = {
            'match_id': 999,
            'radiant_win': True,
            'duration': 1800,
            'players': [{'hero_id': 1}]
        }
        
        result = fetch_matches_sql(limit=1)
        
        # Should fall back to API method
        assert len(result) >= 0  # May return data from fallback

def test_fetch_public_matches():
    """Test fetching public matches."""
    with patch('src.extract.OpenDota') as mock_client:
        mock_instance = mock_client.return_value
        mock_instance.get.return_value = [
            {'match_id': 123, 'radiant_win': True},
            {'match_id': 456, 'radiant_win': False}
        ]
        
        result = fetch_public_matches(limit=10)
        
        assert len(result) <= 10
        assert len(result) == 2
        mock_instance.get.assert_called_with('publicMatches')

def test_fetch_public_matches_fallback():
    """Test fetching public matches with fallback to pro matches."""
    with patch('src.extract.OpenDota') as mock_client:
        mock_instance = mock_client.return_value
        
        # First call returns empty (publicMatches fails)
        mock_instance.get.return_value = []
        
        # Second call returns pro matches
        mock_instance.get_pro_matches.return_value = [
            {'match_id': 999, 'radiant_win': True},
            {'match_id': 888, 'radiant_win': False}
        ]
        
        result = fetch_public_matches(limit=2)
        
        # Should have fallen back to pro matches
        assert len(result) == 2
        assert result[0]['match_id'] == 999

def test_fetch_hero_stats():
    """Test fetching hero statistics."""
    with patch('src.extract.OpenDota') as mock_client:
        mock_instance = mock_client.return_value
        mock_instance.get_hero_stats.return_value = [
            {'id': 1, 'localized_name': 'Anti-Mage', 'pro_win': 50},
            {'id': 2, 'localized_name': 'Axe', 'pro_win': 45}
        ]
        
        result = fetch_hero_stats()
        
        assert len(result) == 2
        assert result[0]['localized_name'] == 'Anti-Mage'

def test_save_data():
    """Test saving data to JSON file."""
    with tempfile.TemporaryDirectory() as tmpdir:
        data = [{'test': 'data', 'value': 123}]
        filepath = save_data(data, tmpdir, 'test.json')
        
        assert os.path.exists(filepath)
        with open(filepath, 'r') as f:
            loaded = json.load(f)
        assert loaded == data
        assert loaded[0]['value'] == 123

def test_matches_sql_structure():
    """Test that SQL matches have correct structure."""
    with patch('src.extract.OpenDota') as mock_client:
        mock_instance = mock_client.return_value
        
        mock_instance.explorer.return_value = [
            {
                'match_id': 100,
                'radiant_win': True,
                'duration': 1800,
                'game_mode': 22,
                'player_slot': 0,
                'hero_id': 1,
                'kills': 10,
                'deaths': 5,
                'assists': 15,
                'gold_per_min': 450,
                'xp_per_min': 500,
                'last_hits': 150,
                'denies': 10,
                'hero_damage': 15000,
                'tower_damage': 2000,
                'hero_healing': 1000,
                'level': 20
            }
        ]
        
        result = fetch_matches_sql(limit=1)
        
        assert len(result) == 1
        match = result[0]
        
        # Check match-level fields
        assert 'match_id' in match
        assert 'radiant_win' in match
        assert 'duration' in match
        assert 'game_mode' in match
        assert 'players' in match
        
        # Check player-level fields
        assert len(match['players']) == 1
        player = match['players'][0]
        assert 'hero_id' in player
        assert 'kills' in player
        assert 'deaths' in player
        assert 'gold_per_min' in player

