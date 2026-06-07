import { useState, useEffect } from "react"
import { useFlowSyncClient } from "./context"

export interface FlowSyncStatus {
  isOnline:     boolean
  pendingCount: number
  connect:      () => Promise<void>
  disconnect:   () => Promise<void>
}

export function useFlowSync(): FlowSyncStatus {
  const client = useFlowSyncClient()
  const [status, setStatus] = useState({
    isOnline: client.isOnline,
    pendingCount: client.pendingCount
  })

  useEffect(() => {
    const updateStatus = () => {
      setStatus({
        isOnline: client.isOnline,
        pendingCount: client.pendingCount
      })
    }

    const unsubConnect = client.onConnect(updateStatus)
    const unsubDisconnect = client.onDisconnect(updateStatus)
    const unsubSync = client.onSyncComplete(updateStatus)

    // Periodic check to capture offline queue changes locally
    const interval = setInterval(updateStatus, 200)

    return () => {
      unsubConnect()
      unsubDisconnect()
      unsubSync()
      clearInterval(interval)
    }
  }, [client])

  return {
    isOnline: status.isOnline,
    pendingCount: status.pendingCount,
    connect: () => client.connect(),
    disconnect: () => client.disconnect()
  }
}
