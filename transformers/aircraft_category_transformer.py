import logging
from typing import Dict, List, Tuple
from statistics import mean
from data_utils import safe_get, clean_string, safe_int


class AircraftCategoryTransformer:
    """Transform and sort aircraft categories by average capacity"""
    
    def __init__(self):
        self.category_capacity_map = {}
        self.sorted_categories = []
    
    def calculate_category_capacities(self, aircraft_data: List[Dict]) -> Dict[str, float]:
        """Calculate mean capacity for each aircraft category based on aircraft data"""
        category_capacities = {}
        
        for aircraft in aircraft_data:
            if safe_get(aircraft, 'active') and safe_get(aircraft, 'managed'):
                aircraft_category = clean_string(safe_get(aircraft, 'aircraftCategory'))
                capacity = safe_int(safe_get(aircraft, 'capacity'))
                
                if aircraft_category and capacity and capacity > 0:
                    if aircraft_category not in category_capacities:
                        category_capacities[aircraft_category] = []
                    category_capacities[aircraft_category].append(capacity)
        
        # Calculate mean capacity for each category
        category_means = {}
        for category, capacities in category_capacities.items():
            category_means[category] = mean(capacities)
            logging.debug(f"Category '{category}': {len(capacities)} aircraft, mean capacity: {category_means[category]:.1f}")
        
        self.category_capacity_map = category_means
        logging.info(f"Calculated capacities for {len(category_means)} aircraft categories")
        
        return category_means
    
    def sort_categories_by_capacity(self, category_data: List[Dict], aircraft_data: List[Dict]) -> List[Tuple[Dict, int, float]]:
        """
        Sort aircraft categories by average capacity and assign auto-increment IDs
        
        Returns:
            List of tuples: (category_dict, auto_increment_id, mean_capacity)
        """
        # First calculate capacities from aircraft data
        self.calculate_category_capacities(aircraft_data)
        
        # Create list of categories with their calculated mean capacities
        categories_with_capacity = []
        missing_capacity_categories = []
        
        for category in category_data:
            category_name = clean_string(safe_get(category, 'name'))
            if category_name in self.category_capacity_map:
                mean_capacity = self.category_capacity_map[category_name]
                categories_with_capacity.append((category, mean_capacity))
            else:
                # Categories without capacity data go to the end
                missing_capacity_categories.append((category, 0.0))
                logging.warning(f"No capacity data found for category: {category_name}")
        
        # Sort by mean capacity (ascending - smallest to largest)
        categories_with_capacity.sort(key=lambda x: x[1])
        
        # Combine sorted categories with missing ones at the end
        all_categories = categories_with_capacity + missing_capacity_categories
        
        # Assign auto-increment IDs starting from 1
        sorted_categories_with_ids = []
        for idx, (category, mean_capacity) in enumerate(all_categories, start=1):
            sorted_categories_with_ids.append((category, idx, mean_capacity))
        
        self.sorted_categories = sorted_categories_with_ids
        
        # Log the sorted order
        logging.info("Aircraft categories sorted by capacity:")
        for category, category_id, mean_capacity in sorted_categories_with_ids:
            category_name = clean_string(safe_get(category, 'name'))
            if mean_capacity > 0:
                logging.info(f"  ID {category_id}: {category_name} (avg capacity: {mean_capacity:.1f})")
            else:
                logging.info(f"  ID {category_id}: {category_name} (no capacity data)")
        
        return sorted_categories_with_ids
    
    def get_category_id_by_name(self, category_name: str) -> int:
        """Get the auto-increment ID for a category by name"""
        clean_name = clean_string(category_name)
        for category, category_id, _ in self.sorted_categories:
            if clean_string(safe_get(category, 'name')) == clean_name:
                return category_id
        return None
    
    def get_sorted_categories(self) -> List[Tuple[Dict, int, float]]:
        """Get the sorted categories list"""
        return self.sorted_categories