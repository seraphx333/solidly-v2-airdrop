import json
import os
from collections import Counter, defaultdict
from functools import wraps
from itertools import zip_longest
from pathlib import Path
import requests
import toml
from web3 import Web3
from brownie import MerkleDistributor,  interface, web3, Contract
from eth_abi.packed import encode_abi_packed
from eth_utils import encode_hex
from toolz import valfilter
from tqdm import trange


# Snapshot block
SNAPSHOT_BLOCK = 48892026
SNAPSHOT_TIMESTAMP = 1665446399

# Constants
SECONDS_PER_DAY = 86400
VALID_LOCK_START_BLOCK = 39600000

# Addresses
VENFT_ADDRESS = "0xcBd8fEa77c2452255f59743f55A3Ea9d83b3c72b"
OXD_ADDRESS = "0xc5A9848b9d145965d821AaeC8fA32aaEE026492d"
SEX_ADDRESS = "0xD31Fcd1f7Ba190dBc75354046F6024A9b86014d7"
OXSOLID_ADDRESS = "0xDA0053F0bEfCbcaC208A3f867BB243716734D809"
SOLID_ADDRESS = "0x888EF71766ca594DED1F0FA3AE64eD2941740A20"
SOLID_SEX_ADDRESS = "0x41adAc6C1Ff52C5e27568f27998d747F7b69795B"
VL_OXD_ADDRESS = "0xDA00527EDAabCe6F97D89aDb10395f719E5559b9"
VL_SEX_ADDRESS = "0xDcC208496B8fcc8E99741df8c6b8856F1ba1C71F"
MINTER_ADDRESS = "0xC4209c19b183e72A037b2D1Fb11fbe522054A90D"
BURN_ADDRESS = "0x12e569CE813d28720894c2A0FFe6bEC3CCD959b2"
BURNING_ESCROW_ADDRESS = "0x16A3a99BEe5cA47a21E6AF9B08e9EcDc56c0a339"
BURN_DELEGATOR_ADDRESS = "0x15D5823b33Ad6c272274a8Dc61E617153AB1da1D"
USER_PROXY_FACTORY_ADDRESS = "0xDA00Aad945d0d5F1B1b3FBb6E0ce3E36827A7bF5"
SOLIDLY_LENS_ADDRESS = "0xDA0024F99A9889E8F48930614c27Ba41DD447c45"

# Multisig
MULTISIG_TEAM_ADDRESS = "0x0000000000000000000000000000000000000001".lower()
MULTISIG_AUCTION_ADDRESS = "0x0000000000000000000000000000000000000002".lower()
MULTISIG_AIRDROP_ADDRESS = "0x0000000000000000000000000000000000000003".lower()
MULTISIG_PARTNER_ADDRESS= "0x0000000000000000000000000000000000000004".lower()
MULTISIG_MONOLITH_ADDRESS = "0x0000000000000000000000000000000000000005".lower()


# Etherscan
ETHERSCAN_API_KEY = "EY9ZA3C2ECCMK1K9XDRG85VS4YRP9KBP9I"

# Staking
SOLIDSEX_STAKING_ADDRESS = "0x7FcE87e203501C3a035CbBc5f0Ee72661976D6E1"
SOLIDSEX_STAKING = Contract(SOLIDSEX_STAKING_ADDRESS)


# Tokens
SYMBOLS = {
    VENFT_ADDRESS: 'veNFT',
    OXSOLID_ADDRESS: 'oxSOLID',
    SOLID_ADDRESS: 'SOLID',
    SOLID_SEX_ADDRESS: 'solidSEX',
    OXD_ADDRESS: 'OXD',
    SEX_ADDRESS: 'SEX'
}
TOKENS = {
    'veNFT': VENFT_ADDRESS,
    'oxSOLID': OXSOLID_ADDRESS,
    'SOLID': SOLID_ADDRESS,
    'solidSEX': SOLID_SEX_ADDRESS,
    'OXD': OXD_ADDRESS,
    'SEX': SEX_ADDRESS,
    'vlOXD': VL_OXD_ADDRESS,
    'vlSEX': VL_SEX_ADDRESS
}

# Remapping
REMAP_ADDRESSES = [
    "0xa96D2F0978E317e7a97aDFf7b5A76F4600916021",
    "0x95478C4F7D22D1048F46100001c2C69D2BA57380",
    "0xC0E2830724C946a6748dDFE09753613cd38f6767",
    "0x3293cB515Dbc8E0A8Ab83f1E5F5f3CC2F6bbc7ba",
    "0xffFfBBB50c131E664Ef375421094995C59808c97",
    "0x02517411F32ac2481753aD3045cA19D58e448A01",
    "0xf332789fae0d1d6f058bfb040b3c060d76d06574",
    "0xdFf234670038dEfB2115Cf103F86dA5fB7CfD2D2",
    "0x0f2A144d711E7390d72BD474653170B201D504C8",
    "0x224002428cF0BA45590e0022DF4b06653058F22F",
    "0x26D70e4871EF565ef8C428e8782F1890B9255367",
    "0xA5fC0BbfcD05827ed582869b7254b6f141BA84Eb",
    "0x4D5362dd18Ea4Ba880c829B0152B7Ba371741E59",
    "0x1e26D95599797f1cD24577ea91D99a9c97cf9C09",
    "0xb4ad8B57Bd6963912c80FCbb6Baea99988543c1c",
    "0xF9E7d4c6d36ca311566f46c81E572102A2DC9F52",
    "0xE838c61635dd1D41952c68E47159329443283d90",
    "0x111731A388743a75CF60CCA7b140C58e41D83635",
    "0x0edfcc1b8d082cd46d13db694b849d7d8151c6d5",
    "0xD0Bb8e4E4Dd5FDCD5D54f78263F5Ec8f33da4C95",
    "0x9685c79e7572faF11220d0F3a1C1ffF8B74fDc65",
    "0xa70b1d5956DAb595E47a1Be7dE8FaA504851D3c5",
    "0x06917EFCE692CAD37A77a50B9BEEF6f4Cdd36422",
    "0x5b0390bccCa1F040d8993eB6e4ce8DeD93721765",
    "0x5180db0237291A6449DdA9ed33aD90a38787621c",
    "0xb2c5548B8EF131921042fB989119d5801a850415",
    "0x982828305ed415a1945b37b5bb5c8e752b9d5770"
]

# Contracts
OXD = Contract(OXD_ADDRESS)
OX_SOLID = Contract(OXSOLID_ADDRESS)
SEX = Contract(SEX_ADDRESS)
SOLID_SEX = Contract(SOLID_SEX_ADDRESS)
SOLID = Contract(SOLID_ADDRESS)
USER_PROXY_FACTORY = Contract(USER_PROXY_FACTORY_ADDRESS)
VL_OXD = Contract(VL_OXD_ADDRESS)
VL_SEX = Contract(VL_SEX_ADDRESS)
BURNING_ESCROW = Contract(BURNING_ESCROW_ADDRESS)
VE_NFT = Contract(VENFT_ADDRESS)
BURN_DELEGATOR = Contract(BURN_DELEGATOR_ADDRESS)
SOLIDLY_LENS = Contract(SOLIDLY_LENS_ADDRESS)
MIGRATION_BURN = Contract(BURN_ADDRESS)

# Sorting
def sortBalances(allBalances):
    for token in allBalances:
        balances = allBalances[token]
        sortedBalances = dict(sorted(balances.items(), key=lambda item: item[1], reverse=True))
        allBalances[token] = sortedBalances
    return allBalances

# Covalent
def usersByTokenTransfers(toAddress, tokenAddress):
    page = 0
    hasMoreResults = True
    addressesMap = {}
    transferCount = 0
    while hasMoreResults == True:
        response = requests.get(f'https://api.covalenthq.com/v1/250/address/{toAddress}/transfers_v2/?contract-address={tokenAddress}&page-size=10000000&page-number={page}', auth=("ckey_199659a1469f461296a1297de7c","")).json()
        hasMoreResults = response.get('data').get('pagination').get('has_more')
        items = response.get('data').get('items')
        for item in items:
            transfers = item.get('transfers')
            for transfer in transfers:
                fromAddress = transfer.get('from_address')
                toAddress = transfer.get('to_address')
                addressesMap[fromAddress] = True
                addressesMap[toAddress] = True
                transferCount += 1
        page += 1
    accountAddresses = [address for address in addressesMap.keys()]
    print("Found:", len(accountAddresses), "addresses")
    return accountAddresses
    
def blockHeightsForAddress(toAddress, fromBlock, toBlock):
    page = 0
    hasMoreResults = True
    results = []
    while hasMoreResults == True:
        response = requests.get(f'https://api.covalenthq.com/v1/250/address/{toAddress}/transactions_v2/?&page-size=1000&page-number={page}', auth=("ckey_199659a1469f461296a1297de7c","")).json()
        hasMoreResults = response.get('data').get('pagination').get('has_more')
        items = response.get('data').get('items')
        for item in items:
            blockHeight = item.get('block_height')
            if blockHeight <= toBlock and blockHeight >= fromBlock:
                results.append(blockHeight)
            if blockHeight < fromBlock:
                hasMoreResults = False
        page += 1
    return results

# Unique addresses
def uniqueAddresses(transactions):
    addressesMap = {}
    for tx in transactions:
        fromAddress = tx.get('from')
        toAddress = tx.get('to')
        addressesMap[fromAddress] = True
        addressesMap[toAddress] = True
    accountAddresses = [address for address in addressesMap.keys()]
    return accountAddresses

# Caching
def cached(path):
    path = Path(path)
    codec = {'.toml': toml, '.json': json}[path.suffix]
    codec_args = {'.json': {'indent': 2}}.get(path.suffix, {})
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            if path.exists():
                print('load from cache', path)
                return codec.loads(path.read_text())
            else:
                result = func(*args, **kwargs)
                os.makedirs(path.parent, exist_ok=True)
                path.write_text(codec.dumps(result, **codec_args))
                print('write to cache', path)
                return result
        return wrapper
    return decorator

@cached('snapshot/01-balances-raw.toml')
def step_01():
    print("step 01. raw balances")
    burnErc20Transactions = requests.get(f'https://api.ftmscan.com/api?module=account&action=tokentx&address={BURN_ADDRESS}&startblock=0&endblock=99999999&sort=asc&apikey={ETHERSCAN_API_KEY}').json()['result']
    burnNftTransactions = requests.get(f'https://api.ftmscan.com/api?module=account&action=tokennfttx&address={BURN_ADDRESS}&startblock=0&endblock=99999999&sort=asc&apikey={ETHERSCAN_API_KEY}').json()['result']
    escrowNftTransactions = requests.get(f'https://api.ftmscan.com/api?module=account&action=tokennfttx&address={BURNING_ESCROW_ADDRESS}&startblock=0&endblock=99999999&sort=asc&apikey={ETHERSCAN_API_KEY}').json()['result']
    tokensAddresses = [SEX_ADDRESS, OXD_ADDRESS, SOLID_SEX_ADDRESS, OXSOLID_ADDRESS, SOLID_ADDRESS]
    
    addressesMap = {}
    for tx in burnErc20Transactions:
        fromAddress = tx.get('from')
        toAddress = tx.get('to')
        addressesMap[fromAddress] = True
        addressesMap[toAddress] = True

    for tx in burnNftTransactions:
        fromAddress = tx.get('from')
        toAddress = tx.get('to')
        addressesMap[fromAddress] = True
        addressesMap[toAddress] = True

    for tx in escrowNftTransactions:
        fromAddress = tx.get('from')
        toAddress = tx.get('to')
        addressesMap[fromAddress] = True
        addressesMap[toAddress] = True
    accountAddresses = [address for address in addressesMap.keys()]
    burnMap = {}
    i = 0
    for accountAddress in accountAddresses:
        tokensBurned = 0
        i += 1
        print()
        print("Check account:", accountAddress, "(" + str(i) + " out of " + str(len(accountAddresses)) + ")")
        burnedByAccount = burnMap.get(accountAddress)
        if (burnedByAccount == None):
            burnMap[accountAddress] = {}

        nftBurnedAmount = int(MIGRATION_BURN.veNftBurnedAmountByAccount(accountAddress))
        if nftBurnedAmount > 0:
            burnMap[accountAddress][VENFT_ADDRESS] = nftBurnedAmount
            print("Found burn:", VENFT_ADDRESS, nftBurnedAmount)
            tokensBurned += 1

        for tokenAddress in tokensAddresses:
            tokenBurnedAmount = int(MIGRATION_BURN.tokensBurnedByAccount(tokenAddress, accountAddress))
            if (tokenBurnedAmount > 0):
                burnMap[accountAddress][tokenAddress] = tokenBurnedAmount
                print("Found burn:", tokenAddress, tokenBurnedAmount)
                tokensBurned += 1
        if tokensBurned == 0:
            print("No tokens burned")  
    allBalances = defaultdict(Counter)
    users = burnMap
    for user in users:
        tokens = users[user]
        for token in tokens:
            tokenSymbol = SYMBOLS.get(token)
            balance = tokens[token]
            allBalances[tokenSymbol][user] = balance
    return sortBalances(allBalances)

@cached('snapshot/02-nft-balances-adjusted.toml')
def step_02(allBalances):
    for account in allBalances['veNFT']:
        i = 0
        nftsForOwner = []
        accountTotal = 0
        while True:
            try:
                nft = MIGRATION_BURN.veNftBurnedIdByIndex(account, i)
                nftsForOwner.append(nft)
                i += 1
            except:
                break
        for nft in nftsForOwner:
            locked = VE_NFT.locked(nft, block_identifier = SNAPSHOT_BLOCK)[0]
            accountTotal += locked
        print('account:', account)
        print('nfts:', nftsForOwner)
        print('locked:', accountTotal)
        print()
        allBalances['veNFT'][account] = accountTotal
    return allBalances

@cached('snapshot/03-balances-after-escrow.toml')
def step_03(allBalances):
    print("step 03. burning escrow")
    escrowBalanceBefore = allBalances['veNFT'].get(BURNING_ESCROW_ADDRESS.lower())
    escrowedNfts = BURNING_ESCROW.getEscrowedNfts()
    totalEscrowed = 0
    for nftId in escrowedNfts:
        owner = BURNING_ESCROW.nftOwners(nftId)
        locked = VE_NFT.locked(nftId)[0]
        totalEscrowed += locked
        userBalanceBefore = allBalances['veNFT'].get(owner.lower().lower())
        if userBalanceBefore == None:
            userBalanceBefore = 0
        allBalances['veNFT'][owner.lower()] = userBalanceBefore + locked
        allBalances['veNFT'][BURNING_ESCROW_ADDRESS.lower()] -= locked
    assert totalEscrowed == escrowBalanceBefore, "Invalid escrow balance before"
    escrowBalanceAfter = allBalances['veNFT'].get(BURNING_ESCROW_ADDRESS.lower())
    assert escrowBalanceAfter == 0, "Invalid escrow balance after"
    return sortBalances(allBalances)
    
@cached('snapshot/04-vloxd.toml')
def step_04():
    print('step 04. vlOXD')
    users = usersByTokenTransfers(VL_OXD_ADDRESS, OXD_ADDRESS)
    balances = Counter()
    i = 0
    for user in users:
        i += 1
        print("Fetch vlOXD balances (" + str(i) + " of " + str(len(users)) + ")")
        owner = ""
        if USER_PROXY_FACTORY.isUserProxy(user):
            owner = interface.UserProxy(user).ownerAddress()
        else:
            owner = user
        locked = VL_OXD.balanceOf(user, block_identifier = SNAPSHOT_BLOCK)
        if locked > 0:
            balances[owner] = int(locked)
            print("Found vlOXD balance:", owner, locked)
    return valfilter(bool, dict(balances.most_common()))
    
@cached('snapshot/05-vlsex.toml')
def step_05():
    print('step 05. vlSEX')
    users = {}
    i = 0
    blockHeights = blockHeightsForAddress(VL_SEX_ADDRESS, VALID_LOCK_START_BLOCK, SNAPSHOT_BLOCK)
    for blockHeight in blockHeights:
        print(str(i) + ' out of ' + str(len(blockHeights)))
        i += 1
        logs = VL_SEX.events.NewLock.getLogs(fromBlock=blockHeight, toBlock=blockHeight)
        for log in logs:
           user = log['args']['user']
           users[user] = True
        logs = VL_SEX.events.NewExitStream.getLogs(fromBlock=blockHeight, toBlock=blockHeight)
        for log in logs:
           user = log['args']['user']
           users[user] = True
        logs = VL_SEX.events.ExtendLock.getLogs(fromBlock=blockHeight, toBlock=blockHeight)
        for log in logs:
           user = log['args']['user']
           users[user] = True
           
           
    # users = {}
    # for start in trange(VALID_LOCK_START_BLOCK, SNAPSHOT_BLOCK, 1000):
    #     end = min(start + 999, SNAPSHOT_BLOCK)
    #     logs = VL_SEX.events.NewLock.getLogs(fromBlock=start, toBlock=end)
    #     for log in logs:
    #        user = log['args']['user']
    #        users[user] = True
    # for start in trange(VALID_LOCK_START_BLOCK, SNAPSHOT_BLOCK, 1000):
    #     end = min(start + 999, SNAPSHOT_BLOCK)
    #     logs = VL_SEX.events.ExtendLock.getLogs(fromBlock=start, toBlock=end)
    #     for log in logs:
    #        user = log['args']['user']
    #        users[user] = True
    balances = Counter()
    i = 0
    for user in users:
        i += 1
        print("Fetch vlSEX balances (" + str(i) + " of " + str(len(users)) + ")")
        balance = VL_SEX.userBalance(user, block_identifier=SNAPSHOT_BLOCK)
        weight = VL_SEX.userWeight(user, block_identifier=SNAPSHOT_BLOCK)
        exitStream = VL_SEX.exitStream(user, block_identifier=SNAPSHOT_BLOCK)
        start = exitStream[0]
        amount = exitStream[1]
        claimed = exitStream[2]
        startTimestamp = SNAPSHOT_TIMESTAMP - (SECONDS_PER_DAY * 8)
        if start >= startTimestamp:
            amountLeft = amount - claimed
            balances[user] += amountLeft
        if balance > 0:
            if weight > 0:
                balances[user] += int(balance)
            else:
                balances[MULTISIG_TEAM_ADDRESS] = int(balance)
            print("Found vlSEX balance:", user, balance)
    return valfilter(bool, dict(balances.most_common()))
    
@cached('snapshot/06-combined.toml')
def step_06(allBalances, vloxd_balances, vlsex_balances):    
    print('step 06. aggregate data')
    
    # vlOXD
    for user in vloxd_balances:
        currentBalance = allBalances['OXD'].get(user)
        if currentBalance == None:
            allBalances['OXD'][user] = 0
        balance = vloxd_balances[user]
        allBalances['OXD'][user] += balance

    # vlSEX
    for user in vlsex_balances:
        currentBalance = allBalances['SEX'].get(user)
        if currentBalance == None:
            allBalances['SEX'][user] = 0
        balance = vlsex_balances[user]
        allBalances['SEX'][user] += balance
        
    # Save balances even though they're already folded into OXD and SEX now for accounting purposes
    allBalances['vlOXD'] = vloxd_balances
    allBalances['vlSEX'] = vlsex_balances

    return sortBalances(allBalances)
    
@cached('snapshot/07-remapped.toml')
def step_07(allBalances):
    print('step 07. protocol remapping')
    recipient = MULTISIG_PARTNER_ADDRESS
    remapAddressesLowerCase = []
    for remapAddress in REMAP_ADDRESSES:
        remapAddressesLowerCase.append(remapAddress.lower())

    for token in allBalances:
        recipientCurrentBalance = allBalances[token].get(recipient)
        if recipientCurrentBalance == None:
            allBalances[token][recipient] = 0
        balances = allBalances[token]
        for user in balances:
            if user in remapAddressesLowerCase:
                balance = balances[user]
                print("Remapping balance", user, token, balance)
                allBalances[token][user] = 0
                allBalances[token][recipient] += balance
    return sortBalances(allBalances)
    
@cached('snapshot/08-with-unburned-part-1.toml')
def step_08(allBalances):
    print("step 08. unburned balances - all")
    merkleTotals = {}

    # Calculate merkle totals
    for token in allBalances:
        balancePerToken = 0
        balances = allBalances[token]
        for user in balances:
            balance = balances[user]
            balancePerToken += balance
        merkleTotals[token] = balancePerToken
    
    print(merkleTotals)
    unburnedNft = SOLID.balanceOf(VENFT_ADDRESS, block_identifier=SNAPSHOT_BLOCK) - merkleTotals['veNFT']
    print("NFT:")
    print("solid.balanceOf(veNFT)", SOLID.balanceOf(VENFT_ADDRESS, block_identifier=SNAPSHOT_BLOCK))
    print("merkleNFT", merkleTotals['veNFT'])
    print("result", unburnedNft)
    print()
    
    unburnedSolid = SOLID.totalSupply(block_identifier=SNAPSHOT_BLOCK) - SOLID.balanceOf(MINTER_ADDRESS, block_identifier=SNAPSHOT_BLOCK) - SOLID.balanceOf(VENFT_ADDRESS, block_identifier=SNAPSHOT_BLOCK) - merkleTotals['SOLID']
    print("SOLID:")
    print("total supply", SOLID.totalSupply(block_identifier=SNAPSHOT_BLOCK))
    print("solid.balanceOf(minter)", SOLID.balanceOf(MINTER_ADDRESS, block_identifier=SNAPSHOT_BLOCK))
    print("solid.balanceOf(veNft)", SOLID.balanceOf(VENFT_ADDRESS, block_identifier=SNAPSHOT_BLOCK))
    print("merkleSolid", merkleTotals['SOLID'])
    print()
    
    unburnedSex = SEX.totalSupply(block_identifier=SNAPSHOT_BLOCK) - merkleTotals['SEX']
    print("SEX")
    print("totalSupply", SEX.totalSupply(block_identifier=SNAPSHOT_BLOCK))
    print("merkleSex", merkleTotals['SEX'])
    print("result", unburnedSex)
    print()
    
    unburnedSolidSex = SOLID_SEX.totalSupply(block_identifier=SNAPSHOT_BLOCK) - merkleTotals['solidSEX']
    print("solidSex")
    print("totalSupply", SOLID_SEX.totalSupply(block_identifier=SNAPSHOT_BLOCK))
    print("merkleSolidSex", merkleTotals['solidSEX'])
    print("result", unburnedSolidSex)
    print()
    
    unburnedOxd = OXD.totalSupply(block_identifier=SNAPSHOT_BLOCK) - merkleTotals['OXD']
    print("OXD")
    print("totalSupply", OXD.totalSupply(block_identifier=SNAPSHOT_BLOCK))
    print("merkleOXD", merkleTotals['OXD'])
    print("result", unburnedOxd)
    print()
    
    unburnedOxSolid = OX_SOLID.totalSupply(block_identifier=SNAPSHOT_BLOCK) - merkleTotals['oxSOLID']
    print("oxSOLID")
    print("totalSupply", OX_SOLID.totalSupply(block_identifier=SNAPSHOT_BLOCK))
    print("merkleOxSolid", merkleTotals['oxSOLID'])
    print("result", unburnedOxSolid)
    print()

    unburnedVlOxd = OXD.balanceOf(VL_OXD_ADDRESS, block_identifier=SNAPSHOT_BLOCK) - merkleTotals['vlOXD']
    print("vlOXD")
    print("oxd.balanceOf(vlOxd)", OXD.balanceOf(VL_OXD_ADDRESS, block_identifier=SNAPSHOT_BLOCK))
    print("merkleVlOxd", merkleTotals['vlOXD'])
    print("result", unburnedVlOxd)
    print()
    
    unburnedVlSex = SEX.balanceOf(VL_SEX_ADDRESS, block_identifier=SNAPSHOT_BLOCK) - merkleTotals['vlSEX']
    print("vlSEX")
    print("sex.balanceOf(vlSex)", SEX.balanceOf(VL_SEX_ADDRESS, block_identifier=SNAPSHOT_BLOCK))
    print("merkleVlSex", merkleTotals['vlSEX'])
    print("result", unburnedVlSex)
    print()

    print("Distribute unburned veNFT:", unburnedNft / 10**18, MULTISIG_AIRDROP_ADDRESS)
    if allBalances['veNFT'].get(MULTISIG_AIRDROP_ADDRESS) == None:
        allBalances['veNFT'][MULTISIG_AIRDROP_ADDRESS] = 0
    allBalances['veNFT'][MULTISIG_AIRDROP_ADDRESS] += unburnedNft

    print("Distribute unburned SOLID:", unburnedSolid / 10**18, MULTISIG_AIRDROP_ADDRESS)
    if allBalances['SOLID'].get(MULTISIG_AIRDROP_ADDRESS) == None:
        allBalances['SOLID'][MULTISIG_AIRDROP_ADDRESS] = 0
    allBalances['SOLID'][MULTISIG_AIRDROP_ADDRESS] += unburnedSolid
    
    print("Distribute unburned SEX:", (unburnedSex + unburnedVlSex) / 10**18, MULTISIG_AIRDROP_ADDRESS)
    if allBalances['SEX'].get(MULTISIG_AIRDROP_ADDRESS) == None:
        allBalances['SEX'][MULTISIG_AIRDROP_ADDRESS] = 0
    allBalances['SEX'][MULTISIG_AIRDROP_ADDRESS] += unburnedVlSex + unburnedSex
    
    print("Distribute unburned OXD:", (unburnedOxd + unburnedVlOxd) / 10**18, MULTISIG_AIRDROP_ADDRESS)
    if allBalances['OXD'].get(MULTISIG_AIRDROP_ADDRESS) == None:
        allBalances['OXD'][MULTISIG_AIRDROP_ADDRESS] = 0
    allBalances['OXD'][MULTISIG_AIRDROP_ADDRESS] += unburnedVlOxd + unburnedOxd
    
    print("Distribute unburned oxSOLID:", unburnedOxSolid / 10**18, MULTISIG_AIRDROP_ADDRESS)
    if allBalances['oxSOLID'].get(MULTISIG_AIRDROP_ADDRESS) == None:
        allBalances['oxSOLID'][MULTISIG_AIRDROP_ADDRESS] = 0
    allBalances['oxSOLID'][MULTISIG_AIRDROP_ADDRESS] += unburnedOxSolid
    
    print("Distribute unburned solidSEX:", unburnedSolidSex / 10**18, MULTISIG_AIRDROP_ADDRESS)
    if allBalances['solidSEX'].get(MULTISIG_AIRDROP_ADDRESS) == None:
        allBalances['solidSEX'][MULTISIG_AIRDROP_ADDRESS] = 0
    allBalances['solidSEX'][MULTISIG_AIRDROP_ADDRESS] += unburnedSolidSex
    return sortBalances(allBalances)

@cached('snapshot/9-with-unburned-part-2.toml')
def step_09(allBalances):
    print("step 09. unburned balances - top 25")
    
    protocolsThatDidntBurnLowercase = []
    for remapAddress in REMAP_ADDRESSES:
        protocolsThatDidntBurnLowercase.append(remapAddress.lower())
    for protocol in protocolsThatDidntBurnLowercase:
        sexBalance = SEX.balanceOf(protocol, block_identifier=SNAPSHOT_BLOCK)
        oxdTotal = OXD.balanceOf(protocol, block_identifier=SNAPSHOT_BLOCK)
        oxSolidTotal = OX_SOLID.balanceOf(protocol, block_identifier=SNAPSHOT_BLOCK)
        solidSexBalance = SOLID_SEX.balanceOf(protocol, block_identifier=SNAPSHOT_BLOCK)
        solidBalance = SOLID.balanceOf(protocol, block_identifier=SNAPSHOT_BLOCK)
        # vlSexTotal = VL_SEX.userBalance(protocol, block_identifier=SNAPSHOT_BLOCK)
        # vlOxdTotal = VL_OXD.balanceOf(protocol, block_identifier = SNAPSHOT_BLOCK)
        veNftTotal = 0
        tokenIds = SOLIDLY_LENS.veTokensIdsOf(protocol)
        for tokenId in tokenIds:
            veNftTotal += VE_NFT.locked(tokenId)[0]
            
        solidSexStake = SOLIDSEX_STAKING.balanceOf(protocol, block_identifier=SNAPSHOT_BLOCK)
        sexEarned = SOLIDSEX_STAKING.earned(protocol, SEX_ADDRESS, block_identifier=SNAPSHOT_BLOCK)
        solidEarned = SOLIDSEX_STAKING.earned(protocol, SOLID_ADDRESS, block_identifier=SNAPSHOT_BLOCK)
        sexTotal = sexBalance + sexEarned
        solidSexTotal = solidSexBalance + solidSexStake
        solidTotal = solidBalance + solidEarned
        print("Protocol: " + protocol)
        print("SEX: ", sexTotal / 10**18)
        print("OXD: ", oxdTotal / 10**18)
        print("oxSOLID: ", oxSolidTotal / 10**18)
        print("solidSEX: ", solidSexTotal / 10**18)
        print("SOLID: ", solidTotal / 10**18)
        print("veNFT:", veNftTotal / 10**18)
        print()
        allBalances['OXD'][MULTISIG_PARTNER_ADDRESS] += oxdTotal
        allBalances['SEX'][MULTISIG_PARTNER_ADDRESS] += sexTotal
        allBalances['solidSEX'][MULTISIG_PARTNER_ADDRESS] += solidSexTotal
        if allBalances['veNFT'].get(MULTISIG_AUCTION_ADDRESS) is None:
            allBalances['veNFT'][MULTISIG_AUCTION_ADDRESS] = 0
        if allBalances['SOLID'].get(MULTISIG_AUCTION_ADDRESS) is None:
            allBalances['SOLID'][MULTISIG_AUCTION_ADDRESS] = 0
        allBalances['veNFT'][MULTISIG_AUCTION_ADDRESS] += veNftTotal
        allBalances['SOLID'][MULTISIG_AUCTION_ADDRESS] += solidTotal
        allBalances['oxSOLID'][MULTISIG_PARTNER_ADDRESS] += oxSolidTotal
        
        allBalances['OXD'][MULTISIG_AIRDROP_ADDRESS] -= oxdTotal
        allBalances['SEX'][MULTISIG_AIRDROP_ADDRESS] -= sexTotal
        allBalances['solidSEX'][MULTISIG_AIRDROP_ADDRESS] -= solidSexTotal
        if allBalances['veNFT'].get(MULTISIG_AIRDROP_ADDRESS) is None:
            allBalances['veNFT'][MULTISIG_AIRDROP_ADDRESS] = 0
        allBalances['veNFT'][MULTISIG_AIRDROP_ADDRESS] -= veNftTotal
        allBalances['SOLID'][MULTISIG_AIRDROP_ADDRESS] -= solidTotal
        allBalances['oxSOLID'][MULTISIG_AIRDROP_ADDRESS] -= oxSolidTotal
        
    return sortBalances(allBalances)
        
@cached('snapshot/10-shifted-balances.toml')
def step_10(allBalances):
    print("step 10. shifting final balances")
    solidexAddress = "0x26e1a0d851cf28e697870e1b7f053b605c8b060f"
    oxdaoAddress = "0xda00ea1c3813658325243e7abb1f1cac628eb582"

    # Initialize
    if allBalances['veNFT'].get(MULTISIG_MONOLITH_ADDRESS) is None:
        allBalances['veNFT'][MULTISIG_MONOLITH_ADDRESS] = 0
    if allBalances['veNFT'].get(MULTISIG_PARTNER_ADDRESS) is None:
        allBalances['veNFT'][MULTISIG_PARTNER_ADDRESS] = 0
    if allBalances['oxSOLID'].get(MULTISIG_PARTNER_ADDRESS) is None:
        allBalances['oxSOLID'][MULTISIG_PARTNER_ADDRESS] = 0
    if allBalances['SOLID'].get(MULTISIG_PARTNER_ADDRESS) is None:
        allBalances['SOLID'][MULTISIG_PARTNER_ADDRESS] = 0
    if allBalances['solidSEX'].get(MULTISIG_PARTNER_ADDRESS) is None:
        allBalances['solidSEX'][MULTISIG_PARTNER_ADDRESS] = 0
    if allBalances['SOLID'].get(MULTISIG_TEAM_ADDRESS) is None:
        allBalances['SOLID'][MULTISIG_TEAM_ADDRESS] = 0
    if allBalances['veNFT'].get(MULTISIG_TEAM_ADDRESS) is None:
        allBalances['veNFT'][MULTISIG_TEAM_ADDRESS] = 0

    # Solidex
    print("Transfering balance of", solidexAddress, "(Solidex) to", MULTISIG_MONOLITH_ADDRESS)
    veSOLIDamount = VE_NFT.locked(8, block_identifier=SNAPSHOT_BLOCK)[0]
    print("Detected", veSOLIDamount, "Solidex NFT")
    allBalances['veNFT'][MULTISIG_MONOLITH_ADDRESS] += veSOLIDamount
    allBalances['veNFT'][MULTISIG_PARTNER_ADDRESS] -= veSOLIDamount

    # 0xDAO
    print("Transfering balance of", oxdaoAddress, "(0xDAO) to", MULTISIG_MONOLITH_ADDRESS)
    veSOLIDamount = allBalances['veNFT'][oxdaoAddress]
    print("Detected", veSOLIDamount, "0xDAO NFT")
    allBalances['veNFT'][MULTISIG_MONOLITH_ADDRESS] += veSOLIDamount
    allBalances['veNFT'][oxdaoAddress] -= veSOLIDamount

    # Splitting burned & unburned partner NFTs from Monolith
    oxSOLIDamount = allBalances['oxSOLID'][MULTISIG_PARTNER_ADDRESS]
    SOLIDsexamount = allBalances['solidSEX'][MULTISIG_PARTNER_ADDRESS]
    DERIVATIVEamount = oxSOLIDamount + SOLIDsexamount

    allBalances['veNFT'][MULTISIG_PARTNER_ADDRESS] += DERIVATIVEamount
    allBalances['veNFT'][MULTISIG_MONOLITH_ADDRESS] -= DERIVATIVEamount

    allBalances['oxSOLID'][MULTISIG_PARTNER_ADDRESS] -= oxSOLIDamount
    allBalances['solidSEX'][MULTISIG_PARTNER_ADDRESS] -= SOLIDsexamount

    # Shift liquid SOLID from partner fund to team Multisig for Genesis Pool
    SHIFTsolid = allBalances['SOLID'][MULTISIG_PARTNER_ADDRESS]
    allBalances['SOLID'][MULTISIG_TEAM_ADDRESS] += SHIFTsolid
    allBalances['SOLID'][MULTISIG_PARTNER_ADDRESS] -= SHIFTsolid

    # Shift 2m veSOLID from partner fund to team Multisig (team allocation)
    SHIFTvesolid = 2000000*10**18
    allBalances['veNFT'][MULTISIG_TEAM_ADDRESS] += SHIFTvesolid
    allBalances['veNFT'][MULTISIG_PARTNER_ADDRESS] -= SHIFTvesolid

    return sortBalances(allBalances)

@cached('snapshot/11-delegated-balances.toml')
def step_11(allBalances):
    print("step 11. delegated balances")
    response = requests.get(f'https://api.covalenthq.com/v1/250/address/{BURN_DELEGATOR_ADDRESS}/transactions_v2/?page-size=1000&page-number=0', auth=("ckey_199659a1469f461296a1297de7c","")).json()
    items = response.get('data').get('items')
    for item in items:
        print(item)
        # print()
        block = item.get('block_height')
        erc20Delegates = BURN_DELEGATOR.events.SetErc20Beneficiary.getLogs(fromBlock=block, toBlock=block)
        nftDelegates = BURN_DELEGATOR.events.SetVeNftBeneficiary.getLogs(fromBlock=block, toBlock=block)
        burns = VE_NFT.events.Transfer.getLogs(fromBlock=block, toBlock=block)

        # TODO: Account for internal burns
        # for burn in burns:
        #     if burn['args']['to'].lower() == BURN_ADDRESS.lower():
        #         print("burn!!", burn)

        for delegate in erc20Delegates:
            fromAddress = delegate['args']['from'].lower()
            beneficiary = delegate['args']['beneficiary'].lower()
            tokenAddress = delegate['args']['tokenAddress']
            symbol = SYMBOLS.get(tokenAddress)
            currentBeneficiaryBalance = allBalances[symbol].get(beneficiary)
            if currentBeneficiaryBalance == None:
                allBalances[symbol][beneficiary] = 0
            allBalances[symbol][beneficiary] += allBalances[symbol][fromAddress]
            allBalances[symbol][fromAddress] = 0
            print("Delegate ERC20 " + symbol + " to " + beneficiary + " from " + fromAddress)
        for delegate in nftDelegates:
            symbol = 'veNFT'
            beneficiary = delegate['args']['beneficiary'].lower()
            tokenId = delegate['args']['tokenId']
            fromAddress = delegate['args']['from'].lower()
            locked = VE_NFT.locked(tokenId, block_identifier=SNAPSHOT_BLOCK)[0]
            currentBeneficiaryBalance = allBalances[symbol].get(beneficiary)
            if currentBeneficiaryBalance == None:
                allBalances[symbol][beneficiary] = 0
            allBalances[symbol][beneficiary] += locked
            allBalances[symbol][fromAddress] -= locked
            
            # TODO: Figure out what is going on with the 3 complex transactions. Yearn, Beefy, unknown
            if allBalances[symbol][fromAddress] < 0:
                allBalances[symbol][fromAddress] = 0
            print("Delegate veNFT " + str(tokenId) + " to " + beneficiary + " from " + fromAddress)
    return sortBalances(allBalances)
        
@cached('snapshot/12-checksummed-totals.toml')
def step_12(allBalances):
    for token in allBalances:
        balances = allBalances[token]
        
        # First find all addresses with no checksums
        usersWithoutChecksums = []
        for user in balances:
            checksum = Web3.toChecksumAddress(user)
            if user != checksum:
                usersWithoutChecksums.append(user)
                
        # For every user without a checksum, check to see if corresponding checksum address exists
        # If not, make one and set initial balnace to zero
        for user in usersWithoutChecksums:
            checksum = Web3.toChecksumAddress(user)
            checksumVal = allBalances[token].get(checksum)
            if checksumVal is None:
                allBalances[token][checksum] = 0
                
        # For every user, if the user is a non-checksum address, add non-checksum balance to checksum user
        balances = allBalances[token]
        for user in balances:
            checksum = Web3.toChecksumAddress(user)
            if user != checksum:
                allBalances[token][checksum] += allBalances[token][user]
                
        # Delete all non-checksum users
        for user in usersWithoutChecksums:
            del allBalances[token][user]
    return sortBalances(allBalances)


class MerkleTree:
    def __init__(self, elements):
        self.elements = sorted(set(web3.keccak(hexstr=el) for el in elements))
        self.layers = MerkleTree.get_layers(self.elements)

    @property
    def root(self):
        return self.layers[-1][0]

    def get_proof(self, el):
        el = web3.keccak(hexstr=el)
        idx = self.elements.index(el)
        proof = []
        for layer in self.layers:
            pair_idx = idx + 1 if idx % 2 == 0 else idx - 1
            if pair_idx < len(layer):
                proof.append(encode_hex(layer[pair_idx]))
            idx //= 2
        return proof

    @staticmethod
    def get_layers(elements):
        layers = [elements]
        while len(layers[-1]) > 1:
            layers.append(MerkleTree.get_next_layer(layers[-1]))
        return layers

    @staticmethod
    def get_next_layer(elements):
        return [MerkleTree.combined_hash(a, b) for a, b in zip_longest(elements[::2], elements[1::2])]

    @staticmethod
    def combined_hash(a, b):
        if a is None:
            return b
        if b is None:
            return a
        return web3.keccak(b''.join(sorted([a, b])))


def calculate_merkle_tree(balances):
    elements = [(index, account, amount) for index, (account, amount) in enumerate(balances.items())]
    nodes = [encode_hex(encode_abi_packed(['uint', 'address', 'uint'], el)) for el in elements]
    tree = MerkleTree(nodes)
    distribution = {
        'merkleRoot': encode_hex(tree.root),
        'tokenTotal': hex(sum(balances.values())),
        'claims': {
            user: {'index': index, 'amount': hex(amount), 'proof': tree.get_proof(nodes[index])}
            for index, user, amount in elements
        },
    }
    print(f'merkle root: {encode_hex(tree.root)}')
    return distribution

@cached('snapshot/merkle-venft-distribution.json')
def merkle_venft(balances):
    return calculate_merkle_tree(balances)

@cached('snapshot/merkle-solid-distribution.json')
def merkle_solid(balances):
    return calculate_merkle_tree(balances)
    
@cached('snapshot/merkle-oxsolid-distribution.json')
def merkle_oxsolid(balances):
    return calculate_merkle_tree(balances)
    
@cached('snapshot/merkle-solidsex-distribution.json')
def merkle_solidsex(balances):
    return calculate_merkle_tree(balances)
    
@cached('snapshot/merkle-oxd-distribution.json')
def merkle_oxd(balances):
    return calculate_merkle_tree(balances)
    
@cached('snapshot/merkle-sex-distribution.json')
def merkle_sex(balances):
    return calculate_merkle_tree(balances)
    
@cached('snapshot/airdrop.json')
def build_merkles(balances):
    print("building merkles...")
    return {
        "veNFT": merkle_venft(balances['veNFT']),
        "oxSOLID": merkle_oxsolid(balances['oxSOLID']),
        "solidSEX": merkle_solidsex(balances['solidSEX']),
        "OXD": merkle_oxd(balances['OXD']),
        "SEX": merkle_sex(balances['SEX']),
        "SOLID": merkle_solid(balances['SOLID'])
    }

def main():
    balances_raw = step_01()
    balances_adjusted = step_02(balances_raw)
    balances_after_escrow = step_03(balances_adjusted)
    vloxd_balances = step_04()
    vlsex_balances = step_05()
    combined_balances = step_06(balances_after_escrow, vloxd_balances, vlsex_balances)
    remapped_balances =step_07(combined_balances)
    remapped_and_unburned_balances_part_1 = step_08(remapped_balances)
    remapped_and_unburned_balances_part_2 = step_09(remapped_and_unburned_balances_part_1)
    shifted_balances = step_10(remapped_and_unburned_balances_part_2)
    delegated_balances = step_11(shifted_balances)
    checksummed_balances = step_12(delegated_balances)
    build_merkles(checksummed_balances)
    
    