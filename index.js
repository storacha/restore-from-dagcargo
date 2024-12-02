import fs from 'node:fs'
import path from 'node:path'
import * as Link from 'multiformats/link'
import { base58btc } from 'multiformats/bases/base58'
import ora from 'ora'
import dotenv from 'dotenv'
import pg from 'pg'
import * as w3up from '@web3-storage/w3up-client'
import { StoreMemory } from '@web3-storage/w3up-client/stores/memory'
import * as Proof from '@web3-storage/w3up-client/proof'
import { Signer } from '@web3-storage/w3up-client/principal/ed25519'
import { findAggregate, downloadURL, downloadAggregate, indexCAR, extractDAG, storeDAG } from './lib.js'

dotenv.config()

const main = async () => {
  if (!process.argv[2]) throw new Error('missing root CID arg')
  const root = Link.parse(process.argv[2])
  console.log(`Restoring ${root} (${base58btc.encode(root.multihash.bytes)})`)

  process.env['NODE_TLS_REJECT_UNAUTHORIZED'] = 0
  const db = new pg.Client({ ssl: true })
  await db.connect()

  const principal = Signer.parse(process.env.PRIVATE_KEY)
  const store = new StoreMemory()
  const storage = await w3up.create({ principal, store })
  const proof = await Proof.parse(process.env.PROOF)
  const space = await storage.addSpace(proof)
  await storage.setCurrentSpace(space.did())

  const spinner = ora()

  const dataPath = path.join(import.meta.dirname, 'data')
  await fs.promises.mkdir(dataPath, { recursive: true })

  const key = await findAggregate(spinner, db, root)
  const srcURL = downloadURL(key)

  const aggDestPath = path.join(dataPath, key)
  if (!fs.existsSync(aggDestPath)) {
    await downloadAggregate(spinner, srcURL, aggDestPath)
  }

  const indexDestPath = `${aggDestPath}.idx`
  if (!fs.existsSync(indexDestPath)) {
    await indexCAR(spinner, aggDestPath, indexDestPath)
  }

  const dagDestPath = path.join(dataPath, `${root}.car`)
  if (!fs.existsSync(dagDestPath)) {
    await extractDAG(spinner, aggDestPath, indexDestPath, root, dagDestPath)
  }

  await storeDAG(spinner, storage, dagDestPath, root)

  await db.end()
}

main()
