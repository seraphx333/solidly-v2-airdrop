import json
from brownie import MerkleDistributor, accounts, Contract


def main():
    with open('snapshot/airdrop.json') as fp:
        rootTree = json.load(fp)
        
    tokens = {
        'veNFT': '0x080f1ED8d2e0D4EcB59d5F1372B8e1F736899Eae',
        'SOLID': '0x080f1ED8d2e0D4EcB59d5F1372B8e1F736899Eae',
        'oxSOLID': '0xE6469bE0A76c1076d4b9e719951636c67401fa81',
        'solidSEX': '0x72AA06eA7Fcb8EeC05b287A9667a8a44688d8B32',
        'OXD': '0x4a70BE6E0c579E5EAF06477132B9ed90bA9370eA',
        'SEX': '0xFbF9E2d4d13A8dFa9935c69559B0B5Cd4720A354'
    }    

    owner = accounts.load("c301")
    for tokenSymbol in tokens:
        print('token', tokenSymbol)
        tree = rootTree[tokenSymbol]
        amount = int(tree['tokenTotal'], 16)
        token = Contract(tokens[tokenSymbol])
        ownerBalance = token.balanceOf(owner)
        if ownerBalance < amount:
            print("Minting", tokenSymbol, amount - token.balanceOf(owner))
            if tokenSymbol != 'SOLID' and tokenSymbol != 'veNFT': # Deployer is not minter :(
                token.mint(owner, amount - token.balanceOf(owner), {"from": owner})
        print("Deploying merkle distributor", tokenSymbol, token.address, tree['merkleRoot'])
        if tokenSymbol != 'veNFT':
            distributor = MerkleDistributor.deploy(token.address, tree['merkleRoot'], {'from': owner})
            token.transfer(distributor, amount, {'from': owner})
