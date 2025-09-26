#!/usr/bin/env python3
"""
Filter Configurations
Declarative filter definitions for all API endpoints
"""

# Filter configurations for different endpoints
FILTER_CONFIGS = {
    'validators': {
        'exact': [
            'status', 
            'jailed',
            'commission_rate'
        ],
        'like': [
            'moniker',
            'operator_address', 
            'consensus_address'
        ],
        'range': [
            ('tokens', 'min_tokens', 'max_tokens'),
            ('commission_rate', 'min_commission_rate', 'max_commission_rate'),
            ('voting_power', 'min_voting_power', 'max_voting_power'),
            ('uptime_percent', 'min_uptime', 'max_uptime'),
            ('missed_blocks_counter', 'min_missed_blocks', 'max_missed_blocks'),
            ('total_delegations', 'min_total_delegations', 'max_total_delegations'),
            ('total_delegators', 'min_total_delegators', 'max_total_delegators')
        ],
        'custom': []
    },
    
    'nodes': {
        'exact': [
            'country',
            'region', 
            'city',
            'org'
        ]
    },
    
    'balances': {
        'exact': [
            'address',
            'date'
        ],
        'range': [
            ('balance_tia', 'min_balance', 'max_balance')
        ],
        'custom': [
            # Custom filter to exclude zero balances by default
            lambda builder, params: builder.add_custom('balance_tia', {'gt': 0}) 
            if params.get('min_balance') is None or params.get('min_balance', 0) <= 0 
            else None
        ]
    },
    
    'releases': {
        'exact': [
            'version',
            'status'
        ],
        'range': [
            ('version_number', 'min_version', 'max_version')
        ],
        'date_range': [
            ('created_at', 'created_after', 'created_before')
        ]
    },
    
    'delegations': {
        'exact': [
            'delegator_address',
            'validator_address',
            'date'
        ],
        'like': [
            'delegator_address',
            'validator_address'
        ],
        'range': [
            ('amount_tia', 'min_amount', 'max_amount'),
        ],
        'date_range': [
            ('date', 'min_date', 'max_date')
        ],
        'custom': [
            # Custom filter for target_date
            lambda builder, params: builder.add_exact('date', params.get('target_date'))
            if params.get('target_date') else None,
            # Custom filter to exclude zero delegations by default
            lambda builder, params: builder.add_custom('amount_tia', {'gt': 0}) 
            if not params.get('include_zero_delegations', False) 
            else None
        ]
    }
}

# Field validation mappings
VALID_FIELDS = {
    'validators': [
        'operator_address', 'consensus_address', 'consensus_pubkey',
        'moniker', 'jailed', 'status', 'tokens', 'commission_rate',
        'voting_power', 'missed_blocks_counter', 'uptime_percent', 
        'total_delegations', 'total_delegators'
    ],
    'nodes': [
        'id', 'peer_id', 'ip', 'city', 'region', 'country', 'lat', 'lon', 'org'
    ],
    'balances': [
        'id', 'address', 'date', 'balance_tia', 'created_at'
    ],
    'releases': [
        'id', 'version', 'version_number', 'status', 'created_at'
    ],
    'delegations': [
        'id', 'delegator_address', 'validator_address', 
        'amount_tia', 'date', 'validator_id', 'created_at',
        # Validator fields (when include_validator_info=True)
        'validator_moniker', 'validator_status', 'validator_tokens',
        'validator_commission_rate', 'validator_uptime_percent'
    ]
}

def get_filter_config(endpoint_name: str) -> dict:
    """Get filter configuration for endpoint"""
    return FILTER_CONFIGS.get(endpoint_name, {})

def get_valid_fields(endpoint_name: str) -> list:
    """Get valid fields for endpoint"""
    return VALID_FIELDS.get(endpoint_name, [])

def validate_field(endpoint_name: str, field: str) -> bool:
    """Validate if field is allowed for endpoint"""
    valid_fields = get_valid_fields(endpoint_name)
    
    # Check if it's a regular field
    if field in valid_fields:
        return True
    
    # Check if it's an aggregated field (like sum_amount_tia, count, avg_field, etc.)
    if _is_aggregated_field(field):
        return True
    
    return False

def _is_aggregated_field(field: str) -> bool:
    """Check if field is an aggregated field"""
    # Common aggregated field patterns
    aggregated_patterns = [
        'sum_', 'avg_', 'min_', 'max_', 'count'
    ]
    
    for pattern in aggregated_patterns:
        if field.startswith(pattern):
            return True
    
    return False
