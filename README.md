# restore-from-dagcargo

Script to find a CID in the dagcargo database, extract the DAG from an aggregate and upload it to Stroacha.

## Usage

Copy `.env.tpl` to `.env` and fill in the required variables.

```sh
node index.js <ROOT_CID>
```
