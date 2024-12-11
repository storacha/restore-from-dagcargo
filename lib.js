import { Readable, Writable } from 'node:stream'
import fs from 'node:fs'
import path from 'node:path'
import crypto from 'node:crypto'
import * as Link from 'multiformats/link'
import { sha256 } from 'multiformats/hashes/sha2'
import * as Digest from 'multiformats/hashes/digest'
import { base58btc } from 'multiformats/bases/base58'
import * as Block from 'multiformats/block'
import { CARReaderStream, CARWriterStream } from 'carstream'
import { ShardedDAGIndex } from '@storacha/blob-index'
import { format as formatBytes } from 'bytes'
import { Decoders, Hashers } from './defaults.js'

/**
 * @import { UnknownLink } from 'multiformats'
 * @import { Ora } from 'ora'
 * @import { Client } from 'pg'
 */

/** @param {UnknownLink} root */
const dagcargoQuery = (root) => `
  SELECT DISTINCT metadata->>'md5hex' || '_' || piece_cid || '.car' AS key
    FROM cargo.aggregate_entries
    JOIN cargo.aggregates USING (aggregate_cid)
   WHERE cid_v1 IN ('${root}')`

/**
 * @param {Ora} spinner
 * @param {Client} db
 * @param {UnknownLink} root
 * @returns {Promise<string>}
 */
export const findAggregate = async (spinner, db, root) => {
  spinner.start(`finding aggregate for ${root}`)
  const res = await db.query(dagcargoQuery(root))
  if (!res.rows.length) {
    spinner.fail()
    throw new Error(`no aggregate found for ${root}`)
  }
  spinner.succeed(`found ${root} in ${res.rows[0].key}`)
  return res.rows[0].key
}

export const downloadURL = key =>
  new URL(`https://roundabout.web3.storage/key/${key}?bucket=dagcargo`)

/**
 * @param {Ora} spinner 
 * @param {URL} srcURL 
 * @param {string} destPath
 */
export const downloadBlob = async (spinner, srcURL, destPath) => {
  spinner.start(`downloading blob from ${srcURL}`)
  const res = await fetch(srcURL)
  if (!res.ok) {
    spinner.fail()
    throw new Error(`failed to download blob: ${res.status}`, { cause: await res.text() })
  }
  let bytes = 0
  let total = parseInt(res.headers.get('Content-Length'))
  const update = () => {
    spinner.text = `downloading blob from ${srcURL} (${formatBytes(bytes)} of ${formatBytes(total)})`
  }
  const intervalID = setInterval(update, 5000)

  const tmpPath = destPath + '.tmp'
  try {
    await res.body
      .pipeThrough(new TransformStream({
        transform (chunk, controller) {
          bytes += chunk.length
          controller.enqueue(chunk)
        }
      }))
      .pipeTo(Writable.toWeb(fs.createWriteStream(tmpPath)))
  } catch (err) {
    // cleanup on error
    if (bytes > 0) await fs.promises.rm(tmpPath)
    throw err
  }
  await fs.promises.rename(tmpPath, destPath)

  clearInterval(intervalID)
  update()
  spinner.succeed()
}

/**
 * @param {Ora} spinner 
 * @param {string} srcPath
 */
export const hashFile = async (spinner, srcPath) => {
  spinner.start(`hashing ${path.basename(srcPath)}`)
  const hash = crypto.createHash('sha256')
  await Readable.toWeb(fs.createReadStream(srcPath))
    .pipeTo(new WritableStream({ write (chunk) { hash.update(chunk) } }))

  const digest = Digest.create(sha256.code, hash.digest())
  spinner.succeed(`hashed ${path.basename(srcPath)} => ${base58btc.encode(digest.bytes)}`)
  return digest
}

/**
 * @param {Ora} spinner 
 * @param {string} srcPath
 * @param {string} destPath
 * @param {UnknownLink} content
 */
export const indexCAR = async (spinner, srcPath, destPath, content = Link.parse('bafkqaaa')) => {
  const shard = await hashFile(spinner, srcPath)
  const index = ShardedDAGIndex.create(content)
  spinner.start(`indexing ${path.basename(srcPath)}`)
  let blocks = 0
  await Readable.toWeb(fs.createReadStream(srcPath))
    .pipeThrough(new CARReaderStream())
    .pipeTo(new WritableStream({
      write (block) {
        blocks++
        index.setSlice(shard, block.cid.multihash, [block.blockOffset, block.blockLength])
        spinner.text = `indexing ${path.basename(srcPath)} (${blocks.toLocaleString()} blocks)`
      }
    }))
  await fs.promises.writeFile(destPath, (await index.archive()).ok)
  spinner.succeed()
}

/**
 * @param {Ora} spinner
 * @param {string} srcPath
 * @param {string} indexPath
 * @param {UnknownLink} root
 * @param {string} destPath
 */
export const extractDAG = async (spinner, srcPath, indexPath, root, destPath) => {
  let shard
  /** @type {import('@storacha/blob-index/types').ShardedDAGIndexView} */
  let index

  spinner.start(`reading index ${path.basename(indexPath)}`)
  const extractResult = ShardedDAGIndex.extract(await fs.promises.readFile(indexPath))
  if (extractResult.error) throw extractResult.error
  index = extractResult.ok
  shard = index.shards.keys().next().value
  spinner.succeed()

  const handle = await fs.promises.open(srcPath, 'r')

  /** @param {UnknownLink} cid */
  const getBlock = async cid => {
    const pos = index.shards.get(shard).get(cid.multihash)
    const bytes = new Uint8Array(pos[1])
    await handle.read({ buffer: bytes, position: pos[0], length: pos[1] })
    return decodeBlock(cid, bytes)
  }

  spinner.start(`extracting DAG ${root}`)
  let blocks = 0
  const queue = [root]
  await new ReadableStream({
    async pull (controller) {
      const cid = queue.shift()
      if (!cid) {
        return controller.close()
      }
      const block = await getBlock(cid)
      for (const [, cid] of block.links()) {
        queue.push(cid)
      }
      controller.enqueue(block)
      blocks++
      spinner.text = `extracting DAG ${root} (${blocks.toLocaleString()} blocks)`
    }
  })
    .pipeThrough(new CARWriterStream([root]))
    .pipeTo(Writable.toWeb(fs.createWriteStream(destPath)))

  await handle.close()
  spinner.succeed()
}

/**
 * @param {UnknownLink} cid
 * @param {Uint8Array} bytes
 */
const decodeBlock = (cid, bytes) => {
  const codec = Decoders[cid.code]
  const hasher = Hashers[cid.multihash.code]
  return Block.create({ bytes, cid, codec, hasher })
}

/**
 * @param {Ora} spinner
 * @param {import('@web3-storage/w3up-client').Client} storage
 * @param {string} srcPath
 */
export const storeDAG = async (spinner, storage, srcPath) => {
  let msg = `storing CAR ${srcPath}`
  spinner.start(msg)
  // const bytes = await fs.promises.readFile(srcPath)
  // const shard = await storage.capability.store.add(bytes)
  // await storage.capability.upload.add(root, [shard])
  const shards = []
  const root = await storage.uploadCAR({ stream: () => Readable.toWeb(fs.createReadStream(srcPath)) }, {
    onShardStored ({ cid }) {
      msg += `\n  ${cid}`
      spinner.text = msg
      shards.push(cid)
    }
  })
  spinner.succeed()
  return { root, shards }
}
