import json
import os
from collections import Counter, defaultdict
from functools import wraps
from itertools import zip_longest
from pathlib import Path
import requests
import toml
from brownie import MerkleDistributor,  interface, web3, Contract
from eth_abi.packed import encode_abi_packed
from eth_utils import encode_hex
from toolz import valfilter, valmap
from tqdm import trange

# Snapshot block
SNAPSHOT_BLOCK = 48892026

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

# Etherscan
ETHERSCAN_API_KEY = "EY9ZA3C2ECCMK1K9XDRG85VS4YRP9KBP9I"

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
    file = open('./snapshot/00-balances.json')
    users = json.load(file)
    allBalances = defaultdict(Counter)
    for user in users:
        tokens = users[user]
        for token in tokens:
            tokenSymbol = SYMBOLS.get(token)
            balance = tokens[token]
            allBalances[tokenSymbol][user] = balance
    return sortBalances(allBalances)
    
@cached('snapshot/02-balances-after-escrow.toml')
def step_02(allBalances):
    print("step 02. burning escrow")
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
    
@cached('snapshot/03-vloxd.toml')
def step_03():
    print('step 03. vlOXD')
    users = usersByTokenTransfers(VL_OXD_ADDRESS, OXD_ADDRESS)
    balances = Counter()
    i = 0
    for user in users:
        i += 1
        print("Fetch vlOXD balances (" + str(i) + " of " + str(len(users)) + ")")
        locked = VL_OXD.lockedBalanceOf(user, block_identifier = SNAPSHOT_BLOCK)
        owner = ""
        if USER_PROXY_FACTORY.isUserProxy(user):
            owner = interface.UserProxy(user).ownerAddress()
        else:
            owner = user
        if locked > 0:
            balances[owner] = int(locked)
            print("Found vlOXD balance:", owner, locked)
    return valfilter(bool, dict(balances.most_common()))
    
@cached('snapshot/04-vlsex.toml')
def step_04():
    print('step 04. vlSEX')
    users = usersByTokenTransfers(VL_SEX_ADDRESS, SEX_ADDRESS)
    balances = Counter()
    i = 0
    for user in users:
        i += 1
        print("Fetch vlSEX balances (" + str(i) + " of " + str(len(users)) + ")")
        locked = VL_SEX.userBalance(user, block_identifier=SNAPSHOT_BLOCK)
        if locked > 0:
            balances[user] = int(locked)
            print("Found vlSEX balance:", user, locked)
    return valfilter(bool, dict(balances.most_common()))
    
@cached('snapshot/05-combined.toml')
def step_05(allBalances, vloxd_balances, vlsex_balances):    
    print('step 05. aggregate data')
    allBalances['vlOXD'] = vloxd_balances
    allBalances['vlSEX'] = vlsex_balances
    return allBalances
    
@cached('snapshot/06-remapped.toml')
def step_06(allBalances):
    print('step 06. protocol remapping')
    recipient = "0x238f1c0AF2f853ab392355516C3b8a0db5B959e5".lower()
    remapAddresses = [
        "0x5bDacBaE440A2F30af96147DE964CC97FE283305",
        "0xDA00eA1c3813658325243e7ABb1f1Cac628Eb582",
        "0x825049dAD292A01078F065FeF2837E69b4f3A40F",
        "0xC0E2830724C946a6748dDFE09753613cd38f6767",
        "0x982828305Ed415A1945B37b5BB5c8E752B9d5770",
        "0xdFf234670038dEfB2115Cf103F86dA5fB7CfD2D2",
        "0x0f2A144d711E7390d72BD474653170B201D504C8",
        "0xA5fC0BbfcD05827ed582869b7254b6f141BA84Eb",
        "0x4D5362dd18Ea4Ba880c829B0152B7Ba371741E59",
        "0xb4ad8B57Bd6963912c80FCbb6Baea99988543c1c",
        "0xE838c61635dd1D41952c68E47159329443283d90",
        "0x5180db0237291A6449DdA9ed33aD90a38787621c",
        "0x111731A388743a75CF60CCA7b140C58e41D83635",
        "0x9685c79e7572faF11220d0F3a1C1ffF8B74fDc65",
        "0x06917EFCE692CAD37A77a50B9BEEF6f4Cdd36422",
        "0x5b0390bccCa1F040d8993eB6e4ce8DeD93721765"
    ]
    remapAddressesLowerCase = [] 
    for remapAddress in remapAddresses:
        remapAddressesLowerCase.append(remapAddress.lower())

    for token in allBalances:
        recipientCurrentBalance = allBalances[token].get(recipient)
        if recipientCurrentBalance == None:
            allBalances[token][recipient] = 0
        balances = allBalances[token]
        for user in balances:
            if user in remapAddressesLowerCase:
                balance = balances[user]
                print("Remapping balance", user, balance, token)
                allBalances[token][user] = 0
                allBalances[token][recipient] += balance
    return sortBalances(allBalances)
    
@cached('snapshot/07-with-unburned.toml')
def step_07(allBalances):
    multisigAirdrop = "0x238f1c0AF2f853ab392355516C3b8a0db5B959e5".lower()
    multisigAuction = "0x238f1c0AF2f853ab392355516C3b8a0db5B959e5".lower()
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

    print("Distribute unburned veNFT:", unburnedNft / 10**18, multisigAuction)
    if allBalances['veNFT'].get(multisigAuction) == None:
        allBalances['veNFT'][multisigAuction] = 0
    allBalances['veNFT'][multisigAuction] += unburnedNft

    print("Distribute unburned SOLID:", unburnedSolid / 10**18, multisigAuction)
    if allBalances['SOLID'].get(multisigAuction) == None:
        allBalances['SOLID'][multisigAuction] = 0
    allBalances['SOLID'][multisigAuction] += unburnedSolid
    
    print("Distribute unburned SEX:", (unburnedSex + unburnedVlSex) / 10**18, multisigAirdrop)
    if allBalances['SEX'].get(multisigAirdrop) == None:
        allBalances['SEX'][multisigAirdrop] = 0
    allBalances['SEX'][multisigAirdrop] += unburnedVlSex + unburnedSex
    
    print("Distribute unburned OXD:", (unburnedOxd + unburnedVlOxd) / 10**18, multisigAirdrop)
    if allBalances['OXD'].get(multisigAirdrop) == None:
        allBalances['OXD'][multisigAirdrop] = 0
    allBalances['OXD'][multisigAirdrop] += unburnedVlOxd + unburnedOxd
    
    print("Distribute unburned oxSOLID:", unburnedOxSolid / 10**18, multisigAirdrop)
    if allBalances['oxSOLID'].get(multisigAirdrop) == None:
        allBalances['oxSOLID'][multisigAirdrop] = 0
    allBalances['oxSOLID'][multisigAirdrop] += unburnedOxSolid
    
    print("Distribute unburned solidSEX:", unburnedSolidSex / 10**18, multisigAirdrop)
    if allBalances['solidSEX'].get(multisigAirdrop) == None:
        allBalances['solidSEX'][multisigAirdrop] = 0
    allBalances['solidSEX'][multisigAirdrop] += unburnedSolidSex
    return allBalances

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


def main():
    balances_raw = step_01()
    balances_after_escrow = step_02(balances_raw)
    vloxd_balances = step_03()
    vlsex_balances = step_04()
    combined_balances = step_05(balances_after_escrow, vloxd_balances, vlsex_balances)
    remapped_balances =step_06(combined_balances)
    remapped_and_unburned_balances = step_07(remapped_balances)
    # merkle_venft(combined_balances['veNFT'])
    # merkle_oxsolid(combined_balances['oxSOLID'])
    # merkle_solidsex(combined_balances['solidSEX'])
    # merkle_oxd(combined_balances['OXD'])
    # merkle_sex(combined_balances['SEX'])
    # merkle_oxsolid(combined_balances['SOLID'])
    