import { useState } from 'react'
import { FlowSyncClient, FlowSyncProvider, useStream, useFlowSync } from 'flowsync-client'

// Instantiate the FlowSync client pointing to our local dev server
const client = new FlowSyncClient({
  url: 'ws://localhost:8765',
  nodeId: 'client-' + Math.random().toString(36).substring(2, 6)
})

function DemoDashboard() {
  const { isOnline, pendingCount, connect, disconnect } = useFlowSync()
  const [activeTab, setActiveTab] = useState<'counter' | 'text' | 'guests'>('counter')

  // Hook up our collaborative stream states
  const [counter, pushCounter] = useStream<number>('counter:value', 0)
  const [text, pushText] = useStream<string>('text:doc', '')
  const [guests, pushGuests] = useStream<string[]>('set:guests', [])

  // Input state for adding new guests
  const [guestName, setGuestName] = useState('')

  // Handle character-based diffing for collaborative CRDT text editing
  const handleTextareaChange = (e: React.ChangeEvent<HTMLTextAreaElement>) => {
    const newVal = e.target.value
    const oldVal = text || ''

    // Compute diff
    let i = 0
    while (i < oldVal.length && i < newVal.length && oldVal[i] === newVal[i]) {
      i++
    }
    let j = 0
    while (
      j < oldVal.length - i &&
      j < newVal.length - i &&
      oldVal[oldVal.length - 1 - j] === newVal[newVal.length - 1 - j]
    ) {
      j++
    }

    const pos = i
    const deletedLen = oldVal.length - i - j
    const insertedText = newVal.substring(i, newVal.length - j)

    if (deletedLen > 0) {
      pushText({ op: 'delete', pos, len: deletedLen } as any)
    }
    if (insertedText.length > 0) {
      pushText({ op: 'insert', pos, char: insertedText } as any)
    }
  }

  const handleAddGuest = (e: React.FormEvent) => {
    e.preventDefault()
    if (!guestName.trim()) return
    pushGuests({ op: 'add', item: guestName.trim() } as any)
    setGuestName('')
  }

  const handleRemoveGuest = (name: string) => {
    pushGuests({ op: 'remove', item: name } as any)
  }

  return (
    <div className="glass-card">
      <div className="bg-orb-1"></div>
      <div className="bg-orb-2"></div>

      <header style={{ marginBottom: '2rem' }}>
        <h1>FlowSync</h1>
        <p className="tagline">Real-time state synchronization & CRDT conflict resolution</p>
      </header>

      {/* Connection controls & Status badge */}
      <section style={{ marginBottom: '2rem' }}>
        <div className="status-badge">
          <span className={`status-dot ${isOnline ? 'online' : 'offline'}`}></span>
          <span>{isOnline ? 'Online' : 'Offline'}</span>
          <span style={{ color: '#6b7280', margin: '0 0.5rem' }}>|</span>
          <span style={{ fontFamily: 'monospace', color: '#a5b4fc' }}>{client.nodeId}</span>
        </div>

        {pendingCount > 0 && (
          <div style={{ marginTop: '0.75rem', color: '#f59e0b', fontSize: '0.875rem', fontWeight: 600 }}>
            ⚠️ {pendingCount} offline changes queued
          </div>
        )}

        <div className="btn-group" style={{ marginTop: '1.25rem' }}>
          {isOnline ? (
            <button className="reset" onClick={() => disconnect()}>
              Go Offline
            </button>
          ) : (
            <button className="primary" onClick={() => connect()}>
              Go Online
            </button>
          )}
        </div>
      </section>

      {/* Tabs navigation */}
      <div className="btn-group" style={{ borderBottom: '1px solid rgba(255, 255, 255, 0.1)', paddingBottom: '1rem' }}>
        <button
          className={activeTab === 'counter' ? 'primary' : ''}
          onClick={() => setActiveTab('counter')}
          style={{ flex: 1 }}
        >
          Counter
        </button>
        <button
          className={activeTab === 'text' ? 'primary' : ''}
          onClick={() => setActiveTab('text')}
          style={{ flex: 1 }}
        >
          Text Notepad
        </button>
        <button
          className={activeTab === 'guests' ? 'primary' : ''}
          onClick={() => setActiveTab('guests')}
          style={{ flex: 1 }}
        >
          Guest List
        </button>
      </div>

      {/* Tab Panels */}
      <div style={{ marginTop: '2rem', minHeight: '220px' }}>
        {activeTab === 'counter' && (
          <div>
            <h2 style={{ fontSize: '1.25rem', marginBottom: '0.5rem', fontWeight: 600 }}>PN-Counter CRDT</h2>
            <div className="counter-value">{counter}</div>
            <div className="btn-group">
              <button onClick={() => pushCounter({ op: 'decrement', amount: 1 } as any)} style={{ fontSize: '1.25rem' }}>
                -
              </button>
              <button className="primary" onClick={() => pushCounter({ op: 'increment', amount: 1 } as any)} style={{ fontSize: '1.25rem' }}>
                +
              </button>
            </div>
            <p className="footer-note">Increments/decrements are tracked per node and combined using CRDT state merges.</p>
          </div>
        )}

        {activeTab === 'text' && (
          <div>
            <h2 style={{ fontSize: '1.25rem', marginBottom: '1rem', fontWeight: 600 }}>Sequence CRDT Notepad</h2>
            <textarea
              value={text || ''}
              onChange={handleTextareaChange}
              placeholder="Start typing..."
              rows={6}
              style={{
                width: '100%',
                background: 'rgba(255, 255, 255, 0.05)',
                border: '1px solid rgba(255, 255, 255, 0.1)',
                borderRadius: '12px',
                padding: '1rem',
                color: '#fff',
                fontSize: '1rem',
                fontFamily: 'Outfit, sans-serif',
                resize: 'vertical',
                boxSizing: 'border-box'
              }}
            />
            <p className="footer-note">Characters inserts and deletes are synced individually so conflicts resolve gracefully.</p>
          </div>
        )}

        {activeTab === 'guests' && (
          <div>
            <h2 style={{ fontSize: '1.25rem', marginBottom: '1rem', fontWeight: 600 }}>2P-Set CRDT Guest List</h2>
            <form onSubmit={handleAddGuest} className="btn-group" style={{ marginBottom: '1.5rem' }}>
              <input
                type="text"
                value={guestName}
                onChange={(e) => setGuestName(e.target.value)}
                placeholder="Enter guest name..."
                style={{
                  flex: 1,
                  background: 'rgba(255, 255, 255, 0.05)',
                  border: '1px solid rgba(255, 255, 255, 0.1)',
                  borderRadius: '12px',
                  padding: '0.75rem 1rem',
                  color: '#fff',
                  fontSize: '1rem'
                }}
              />
              <button type="submit" className="primary">Add</button>
            </form>

            <ul style={{ listStyle: 'none', padding: 0, margin: 0, textAlign: 'left', maxHeight: '180px', overflowY: 'auto' }}>
              {guests.map((guest, idx) => (
                <li
                  key={idx}
                  style={{
                    display: 'flex',
                    justifyContent: 'space-between',
                    alignItems: 'center',
                    padding: '0.5rem 1rem',
                    background: 'rgba(255, 255, 255, 0.03)',
                    border: '1px solid rgba(255, 255, 255, 0.05)',
                    borderRadius: '8px',
                    marginBottom: '0.5rem'
                  }}
                >
                  <span>{guest}</span>
                  <button
                    onClick={() => handleRemoveGuest(guest)}
                    className="reset"
                    style={{ padding: '0.25rem 0.5rem', minWidth: 'auto', fontSize: '0.8rem' }}
                  >
                    Remove
                  </button>
                </li>
              ))}
              {guests.length === 0 && (
                <li style={{ color: '#6b7280', textAlign: 'center', fontStyle: 'italic', padding: '1rem' }}>
                  No guests added yet
                </li>
              )}
            </ul>
          </div>
        )}
      </div>

      <div className="footer-note" style={{ marginTop: '2.5rem', borderTop: '1px solid rgba(255, 255, 255, 0.05)', paddingTop: '1.5rem' }}>
        Open this app in multiple tabs or browsers to test real-time state sync. Turn off connection to test offline queues, queue-replay, and conflict resolution!
      </div>
    </div>
  )
}

export default function App() {
  return (
    <FlowSyncProvider client={client}>
      <DemoDashboard />
    </FlowSyncProvider>
  )
}
