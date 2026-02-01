import { Injectable, OnModuleInit } from '@nestjs/common'
import { DataService } from '../data-service/data-service.interface'

@Injectable()
export class SqliteService implements OnModuleInit, DataService {
  private db = null
  private sqlite3 = null
  constructor() {}

  async onModuleInit() {
    // Only initialize sqlite when explicitly configured to use it.
    if (typeof process !== 'undefined' && process.env.DATA_STORE !== 'sqlite') {
      return
    }

    const fs = require('fs')
    const path = require('path')

    const dbPath =
      (typeof process !== 'undefined' && process.env.DATABASE_PATH) || './data/database.sqlite'
    const dbDir = path.dirname(dbPath)

    try {
      // Ensure the directory exists
      if (!fs.existsSync(dbDir)) {
        fs.mkdirSync(dbDir, { recursive: true })
      }

      // Lazy-require sqlite3 so native bindings are not loaded when using Redis
      this.sqlite3 = require('sqlite3')

      // Properly wait for the database to open
      this.db = await new Promise((resolve, reject) => {
        const db = new this.sqlite3.Database(dbPath, (err) => {
          if (err) {
            reject(err)
          } else {
            resolve(db)
          }
        })
      })

      // Properly wait for table and index creation
      await this.runQuery('CREATE TABLE IF NOT EXISTS kv (key TEXT PRIMARY KEY, value TEXT)')
      await this.runQuery('CREATE INDEX IF NOT EXISTS key_index ON kv (key)')

      console.log(`Sqlite database initialized at ${dbPath}`)
    } catch (e) {
      console.error('Failed to initialize sqlite database', e)
    }
  }

  private runQuery(query: string, params: any[] = []): Promise<void> {
    return new Promise((resolve, reject) => {
      if (!this.db) {
        return reject(new Error('Sqlite database not initialized'))
      }
      this.db.run(query, params, (err) => {
        if (err) {
          reject(err)
        } else {
          resolve()
        }
      })
    })
  }

  async get<T>(key: string): Promise<T> {
    if (!this.db) throw new Error('Sqlite database not initialized')
    return new Promise((resolve, reject) => {
      this.db.get('SELECT value FROM kv WHERE key = ?', [key], (err, row) => {
        if (err) {
          reject(err)
        } else {
          resolve(row ? JSON.parse(row.value) : null)
        }
      })
    })
  }

  async set(key: string, value: any): Promise<void> {
    if (!this.db) throw new Error('Sqlite database not initialized')
    return new Promise((resolve, reject) => {
      this.db.run(
        'INSERT INTO kv (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = ?',
        [key, JSON.stringify(value), JSON.stringify(value)],
        (err) => {
          if (err) {
            reject(err)
          } else {
            resolve()
          }
        }
      )
    })
  }

  async delete(key: string): Promise<void> {
    if (!this.db) throw new Error('Sqlite database not initialized')
    return new Promise((resolve, reject) => {
      this.db.run('DELETE FROM kv WHERE key = ?', [key], (err) => {
        if (err) {
          reject(err)
        } else {
          resolve()
        }
      })
    })
  }
}
