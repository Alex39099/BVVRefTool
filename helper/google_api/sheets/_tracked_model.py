class TrackedModel:
    _WILDCARD_FIELD = "*"
    _dirty_fields: set[str]
    
    def __init__(self, is_dirty: bool = False):
        self._dirty_fields = set()
        if is_dirty:
            self._dirty_fields.add(self._WILDCARD_FIELD)
        
    def _mark_dirty(self, field_name: str):
        self._dirty_fields.add(field_name)
        
    def mark_clean(self):
        """ Marks this instance clean (=synced to cloud) """
        self._dirty_fields.clear()
        
    @property
    def is_dirty(self):
        return len(self._dirty_fields) != 0
        
    @property
    def dirty_field_mask(self) -> str:
        """ Returns the field mask of all dirty fields

        Returns:
            str: field mask for batchUpdate requests
        """
        if self._WILDCARD_FIELD in self._dirty_fields:
            return self._WILDCARD_FIELD
        field_mask = ",".join(self._dirty_fields)
        return field_mask
