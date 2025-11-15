# Celestia CLI Commands Reference

This document contains CLI commands that can be executed on Celestia nodes to retrieve data and perform operations.

## Important Note

**All commands in this documentation use placeholders (e.g., `<NODE_URI>`, `<CHAIN_ID>`, `<ADDRESS>`). When providing examples, prioritize MAINNET by default:**
- **Mainnet RPC endpoint**: `https://rpc.celestia.pops.one:443`
- **Mainnet gRPC endpoint**: `public-celestia-grpc.numia.xyz:9090`
- **Mainnet chain-id**: `celestia`
- **Testnet (Mocha) chain-id**: `mocha-4` (only use if specifically requested)

## Public gRPC Endpoints

### Testnet (Mocha)
- **ITRocket**: `celestia-testnet-grpc.itrocket.net:443`
- **POPS**: `grpc-mocha.pops.one:9090`
- **Celestia Official**: `grpc.celestia-mocha.com:443`
- **BrightlyStake**: `celestia-testnet.brightlystake.com:9390`
- **Mzonder**: `grpc-celestia-testnet.mzonder.com:443`

### Mainnet
- **Numia**: `public-celestia-grpc.numia.xyz:9090`
- **ITRocket**: `celestia-mainnet-grpc.itrocket.net:443`
- **Cumulo**: `celestia.grpc.cumulo.org.es:443`
- **Stakin**: `celestia.grpc.stakin-nodes.com:443`
- **Noders**: `celestia-grpc.noders.services:11090`

## Bank Operations

### Check Balance
```bash
celestia-appd q bank balances <ADDRESS> --node <NODE_URI>
```
Check wallet balance for a specific address.

### Send Tokens
```bash
celestia-appd tx bank send <FROM_ADDRESS> <TO_ADDRESS> <amount> \
  --node <NODE_URI> --chain-id <CHAIN_ID>
```
Transfer tokens between wallets.

### Multi-Send Tokens
```bash
celestia-appd tx bank multi-send <FROM_ADDRESS> <TO_ADDRESS1> <amount1> <TO_ADDRESS2> <amount2> \
  --node <NODE_URI> --chain-id <CHAIN_ID>
```
Send tokens to multiple recipients in a single transaction.

## Staking Operations

### Delegate Tokens
```bash
celestia-appd tx staking delegate <validator_valoper> <amount> \
  --from <wallet> --chain-id <chain-id>
```
Delegate tokens to a validator.

### Unbond Delegation
```bash
celestia-appd tx staking unbond <validator_valoper> <amount> \
  --from <wallet> --chain-id <chain-id>
```
Cancel delegation (unbond tokens).

### Redelegate
```bash
celestia-appd tx staking redelegate <src_validator> <dst_validator> <amount> \
  --from <wallet> --chain-id <chain-id>
```
Redelegate tokens from one validator to another.

### Cancel Unbonding Delegation
```bash
celestia-appd tx staking cancel-unbonding-delegation <validator_valoper> <amount> <height> \
  --from <wallet> --chain-id <chain-id>
```
Cancel or stop the unbonding process.

## Distribution Operations

### Withdraw Delegator Rewards
```bash
celestia-appd tx distribution withdraw-rewards <validator_valoper> \
  --from <wallet> --chain-id <chain-id>
```
Withdraw delegator rewards from a validator.

### Set Withdrawal Address
```bash
celestia-appd tx distribution set-withdraw-addr <withdraw_address> \
  --from <wallet> --chain-id <chain-id>
```
Set the address for reward withdrawals.

### Withdraw Validator Commission
```bash
celestia-appd tx distribution withdraw-rewards <validator_valoper> \
  --commission --from <validator_wallet> --chain-id <chain-id> --gas auto -y
```
Withdraw validator commission rewards.

## Query Commands

### Validator Delegations
```bash
celestia-appd q staking delegations-to <VALIDATOR_ADDRESS> --node <GRPC_ENDPOINT>
```
Get all delegations to a specific validator.

### Unbonding Delegations
```bash
celestia-appd q staking unbonding-delegations <DELEGATOR_ADDRESS> --node <GRPC_ENDPOINT>
```
Get all unbonding delegations for a specific wallet.

### Redelegations
```bash
celestia-appd q staking redelegations <DELEGATOR_ADDRESS> --node <GRPC_ENDPOINT>
```
Get all redelegations for a specific wallet.

### Delegator Rewards (All Validators)
```bash
celestia-appd q distribution rewards <DELEGATOR_ADDRESS> --node <GRPC_ENDPOINT>
```
Get rewards from all validators for a delegator.

### Delegator Rewards (Specific Validator)
```bash
celestia-appd q distribution rewards <DELEGATOR_ADDRESS> <VALIDATOR_ADDRESS> --node <GRPC_ENDPOINT>
```
Get rewards from a specific validator.

### Validator Information
```bash
celestia-appd q staking validator <VALIDATOR_ADDRESS> --node <GRPC_ENDPOINT>
```
Get information about a specific validator.

### List All Validators
```bash
celestia-appd q staking validators --node <GRPC_ENDPOINT>
```
Get a list of all validators.

### Staking Parameters
```bash
celestia-appd q staking params --node <GRPC_ENDPOINT>
```
Get staking parameters.

### Latest Block
```bash
celestia-appd q block --node <GRPC_ENDPOINT>
```
Get the latest block height.

### Block by Height
```bash
celestia-appd q block <HEIGHT> --node <GRPC_ENDPOINT>
```
Get a specific block by height.

## gRPC Operations

### Check Sync Status
```bash
grpcurl -plaintext <GRPC_ENDPOINT> cosmos.base.tendermint.v1beta1.Service/GetSyncing
```
Get node synchronization status.

### List Available Services
```bash
grpcurl -plaintext <GRPC_ENDPOINT> list
```
Show all available gRPC services.

### List Service Methods
```bash
grpcurl -plaintext <GRPC_ENDPOINT> list cosmos.base.tendermint.v1beta1.Service
```
Show available methods for a specific service.

## Address Parsing

### Parse Operator Address
```bash
celestia-appd keys parse <VALIDATOR_OPERATOR_ADDRESS>
```
Parse validator operator address.

### Get Validator Consensus Address
```bash
celestia-appd tendermint show-address
```
Get validator consensus address from local key.

## Slashing Information

### Signing Infos
```bash
celestia-appd q slashing signing-infos --node <RPC_ENDPOINT>
```
Check all signing information.

### Validator Uptime Calculation Script
```bash
#!/bin/bash

VALCONS_ADDRESS="celestiavalcons1xxxxx"
RPC_ENDPOINT="https://rpc-mocha.pops.one:443"

# Get signing information
SIGNING_INFO=$(celestia-appd q slashing signing-info $VALCONS_ADDRESS --node $RPC_ENDPOINT -o json)

# Extract data
MISSED_BLOCKS=$(echo $SIGNING_INFO | jq -r '.missed_blocks_counter')
INDEX_OFFSET=$(echo $SIGNING_INFO | jq -r '.index_offset')
START_HEIGHT=$(echo $SIGNING_INFO | jq -r '.start_height')

# Get slashing parameters
PARAMS=$(celestia-appd q slashing params --node $RPC_ENDPOINT -o json)
WINDOW=$(echo $PARAMS | jq -r '.signed_blocks_window')

# Calculate uptime
SIGNED_BLOCKS=$((WINDOW - MISSED_BLOCKS))
UPTIME=$(echo "scale=2; ($SIGNED_BLOCKS / $WINDOW) * 100" | bc)

echo "=== Validator Uptime Stats ==="
echo "Missed blocks: $MISSED_BLOCKS"
echo "Window size: $WINDOW"
echo "Signed blocks: $SIGNED_BLOCKS"
echo "Uptime: $UPTIME%"
echo "Start height: $START_HEIGHT"
```

## Transaction Queries

### Get Transaction by Hash (CLI)
```bash
celestia-appd q tx <TX_HASH> --node <RPC_ENDPOINT>
```

### Get Transaction by Hash (REST API)
```bash
curl -s https://api-mocha.pops.one/cosmos/tx/v1beta1/txs/<TX_HASH>
```

### Get Transaction by Hash (RPC)
```bash
curl -s https://rpc-mocha.pops.one/tx?hash=0x<TX_HASH>
```

### Search Transactions by Events

#### By Sender
```bash
celestia-appd q txs --events 'message.sender=<ADDRESS>' --node <RPC_ENDPOINT>
```

#### By Recipient
```bash
celestia-appd q txs --events 'transfer.recipient=<ADDRESS>' --node <RPC_ENDPOINT>
```

#### By Block Height
```bash
celestia-appd q txs --events 'tx.height=<HEIGHT>' --node <RPC_ENDPOINT>
```

### Get All Transactions for an Address

#### Mainnet
```bash
curl -s "https://celestia-rest.publicnode.com/cosmos/tx/v1beta1/txs?events=message.sender='celestia1xxxxx'&pagination.limit=100" | jq
```

#### Mocha Testnet
```bash
curl -s "https://api-mocha.pops.one/cosmos/tx/v1beta1/txs?events=message.sender='celestia1xxxxx'&pagination.limit=100" | jq
```

## Transaction Submission

### Send Tokens Transaction
```bash
celestia-appd tx bank send <FROM> <TO> <AMOUNT>utia \
  --node https://rpc-mocha.pops.one:443 \
  --chain-id mocha-4 \
  --fees 20000utia
```

### Pay for Blob Transaction
```bash
celestia-appd tx blob pay-for-blob <NAMESPACE> <DATA> \
  --node https://rpc-mocha.pops.one:443 \
  --chain-id mocha-4 \
  --fees 20000utia
```

## Notes

- Replace `<ADDRESS>`, `<NODE_URI>`, `<CHAIN_ID>`, `<GRPC_ENDPOINT>`, `<RPC_ENDPOINT>`, etc. with actual values
- For testnet (Mocha), use `chain-id: mocha-4`
- For mainnet, use the appropriate mainnet chain-id
- Amounts can be specified in `utia` (micro TIA) or `TIA` (1 TIA = 1,000,000 utia)
- Always verify the endpoint and chain-id before executing transactions

