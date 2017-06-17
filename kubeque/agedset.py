class AgedSet:
    def __init__(self):
        self.timestamp_per_item = {}

    def update(self, items, timestamp):
        items = set(items)
        existing = set(self.timestamp_per_item.keys())
        new_items = items.difference(existing)
        dropped_items =  existing.difference(items)
        for item in dropped_items:
            del self.timestamp_per_item[item]
        for item in new_items:
            self.timestamp_per_item[item] = timestamp

    def find_older_than(self, timestamp):
        old_items = []
        for item, item_timestamp in self.timestamp_per_item.items():
            if item_timestamp < timestamp:
                old_items.append(item)
        return old_items

    def is_older_than(self, item, timestamp):
        if item not in self.timestamp_per_item:
            return False
        return self.timestamp_per_item[item] < timestamp
