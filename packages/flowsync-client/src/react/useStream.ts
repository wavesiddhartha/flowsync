import { useState, useEffect, useCallback, useRef } from "react"
import { useFlowSyncClient } from "./context"

export function useStream<T>(
  streamName: string,
  initial: T
): [T, (value: T) => Promise<void>] {
  const client = useFlowSyncClient()
  const [value, setValue] = useState<T>(initial)
  const isMountedRef = useRef(true)

  useEffect(() => {
    if (typeof window === "undefined") return
    isMountedRef.current = true
    return () => {
      isMountedRef.current = false
    }
  }, [])

  useEffect(() => {
    if (typeof window === "undefined") return
    // Attempt to load current cached value from client
    client
      .get(streamName)
      .then((val) => {
        if (isMountedRef.current) {
          setValue(val as T)
        }
      })
      .catch(() => {
        // Fall back to initial value if offline or not yet initialized
      })

    // Listen to updates
    const unsubscribe = client.on(streamName, (v) => {
      if (isMountedRef.current) {
        setValue(v as T)
      }
    })

    return unsubscribe
  }, [client, streamName])

  const push = useCallback(
    async (val: T) => {
      await client.push(streamName, val)
    },
    [client, streamName]
  )

  return [value, push]
}
