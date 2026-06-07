import React, { createContext, useContext, ReactNode } from "react"
import { FlowSyncClient } from "../client"

const FlowSyncContext = createContext<FlowSyncClient | null>(null)

export function FlowSyncProvider({
  client,
  children
}: {
  client: FlowSyncClient
  children: ReactNode
}) {
  return (
    <FlowSyncContext.Provider value={client}>
      {children}
    </FlowSyncContext.Provider>
  )
}

export function useFlowSyncClient(): FlowSyncClient {
  const client = useContext(FlowSyncContext)
  if (!client) {
    throw new Error("useFlowSyncClient must be used inside <FlowSyncProvider>")
  }
  return client
}
