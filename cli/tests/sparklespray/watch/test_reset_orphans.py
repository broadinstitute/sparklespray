import pytest
from unittest.mock import patch
from sparklespray.watch.reset_orphans import StablizedSet, StablizedSetElement


class TestStablizedSet:
    def test_init_default_values(self):
        """Test that StablizedSet initializes with correct default values."""
        s = StablizedSet()
        assert s.min_update_count == 2
        assert s.min_time_elapsed == 30
        assert s.latest_values == []
        assert s.per_value == {}

    def test_init_custom_values(self):
        """Test that StablizedSet initializes with custom values."""
        s = StablizedSet(min_update_count=5, min_time_elapsed=60)
        assert s.min_update_count == 5
        assert s.min_time_elapsed == 60

    def test_single_update_no_values_returned(self):
        """Test that a single update doesn't return any values (needs min_update_count=2)."""
        s = StablizedSet(min_update_count=2, min_time_elapsed=0)
        
        with patch('sparklespray.watch.reset_orphans.time', return_value=100.0):
            s.update(['a', 'b', 'c'])
        
        # Should be empty because min_update_count=2 but we only updated once
        assert s.values() == []
        
        # Check internal state
        assert len(s.per_value) == 3
        assert s.per_value['a'].update_count == 1
        assert s.per_value['b'].update_count == 1
        assert s.per_value['c'].update_count == 1

    def test_multiple_updates_same_values(self):
        """Test that values appear after sufficient updates."""
        s = StablizedSet(min_update_count=2, min_time_elapsed=0)
        
        with patch('sparklespray.watch.reset_orphans.time', return_value=100.0):
            s.update(['a', 'b'])
            s.update(['a', 'b'])
        
        # Should return values after 2 updates
        values = s.values()
        assert set(values) == {'a', 'b'}
        
        # Check update counts
        assert s.per_value['a'].update_count == 2
        assert s.per_value['b'].update_count == 2

    def test_values_removed_when_missing(self):
        """Test that values are removed from tracking when not in subsequent updates."""
        s = StablizedSet(min_update_count=2, min_time_elapsed=0)
        
        with patch('sparklespray.watch.reset_orphans.time', return_value=100.0):
            s.update(['a', 'b', 'c'])
            s.update(['a', 'b'])  # 'c' is missing
        
        # 'c' should be removed from per_value
        assert 'c' not in s.per_value
        assert set(s.per_value.keys()) == {'a', 'b'}
        
        # 'a' and 'b' should have update_count=2
        assert s.per_value['a'].update_count == 2
        assert s.per_value['b'].update_count == 2

    def test_time_elapsed_requirement(self):
        """Test that values must be present for minimum time before being returned."""
        s = StablizedSet(min_update_count=2, min_time_elapsed=30)
        
        # First update at time 100
        with patch('sparklespray.watch.reset_orphans.time', return_value=100.0):
            s.update(['a', 'b'])
        
        # Second update at time 120 (only 20 seconds elapsed)
        with patch('sparklespray.watch.reset_orphans.time', return_value=120.0):
            s.update(['a', 'b'])
        
        # Should be empty because not enough time has elapsed
        assert s.values() == []
        
        # Third update at time 140 (40 seconds elapsed from first)
        with patch('sparklespray.watch.reset_orphans.time', return_value=140.0):
            s.update(['a', 'b'])
        
        # Now should return values
        values = s.values()
        assert set(values) == {'a', 'b'}

    def test_mixed_timing_scenarios(self):
        """Test scenario where some values meet criteria and others don't."""
        s = StablizedSet(min_update_count=2, min_time_elapsed=30)
        
        # First update at time 100
        with patch('sparklespray.watch.reset_orphans.time', return_value=100.0):
            s.update(['a', 'b'])
        
        # Second update at time 140 - add 'c' which is new
        with patch('sparklespray.watch.reset_orphans.time', return_value=140.0):
            s.update(['a', 'b', 'c'])
        
        # 'a' and 'b' should meet both criteria, 'c' should not (only 1 update)
        values = s.values()
        assert set(values) == {'a', 'b'}
        assert 'c' not in values

    def test_empty_updates(self):
        """Test behavior with empty updates."""
        s = StablizedSet(min_update_count=2, min_time_elapsed=0)
        
        with patch('sparklespray.watch.reset_orphans.time', return_value=100.0):
            s.update(['a', 'b'])
            s.update([])  # Empty update should clear everything
        
        assert s.values() == []
        assert s.per_value == {}

    def test_values_reappearing(self):
        """Test that values can reappear and restart their tracking."""
        s = StablizedSet(min_update_count=2, min_time_elapsed=0)
        
        with patch('sparklespray.watch.reset_orphans.time', return_value=100.0):
            s.update(['a'])
            s.update(['a'])  # 'a' now qualifies
            
            assert s.values() == ['a']
            
            s.update([])  # Remove 'a'
            assert s.values() == []
            assert 'a' not in s.per_value
            
            s.update(['a'])  # 'a' reappears, starts over
            assert s.values() == []  # Doesn't qualify yet
            assert s.per_value['a'].update_count == 1

    def test_first_update_timestamp_recorded(self):
        """Test that first_update timestamp is recorded correctly."""
        s = StablizedSet()
        
        with patch('sparklespray.watch.reset_orphans.time', return_value=123.45):
            s.update(['test'])
        
        assert s.per_value['test'].first_update == 123.45
        assert s.per_value['test'].update_count == 1

    def test_update_count_increments_correctly(self):
        """Test that update_count increments properly across multiple updates."""
        s = StablizedSet(min_update_count=5, min_time_elapsed=0)
        
        with patch('sparklespray.watch.reset_orphans.time', return_value=100.0):
            for i in range(6):
                s.update(['persistent'])
                expected_count = i + 1
                assert s.per_value['persistent'].update_count == expected_count
                
                if expected_count >= 5:
                    assert 'persistent' in s.values()
                else:
                    assert 'persistent' not in s.values()


class TestStablizedSetElement:
    def test_init(self):
        """Test StablizedSetElement initialization."""
        element = StablizedSetElement(update_count=5, first_update=123.45)
        assert element.update_count == 5
        assert element.first_update == 123.45
