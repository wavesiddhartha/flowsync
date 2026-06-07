import { useState, useEffect, useRef } from "react"
import { useFlowSyncClient } from "./context"

export function useStreamValue<T>(
  streamName: string,
  initial: T
): T {
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
    client
      .get(streamName)
      .then((val) => {
        if (isMountedRef.current) {
          setValue(val as T)
        }
      })
      .catch(() => {})

    const unsubscribe = client.on(streamName, (v) => {
      if (isMountedRef.current) {
        setValue(v as T)
      }
    })

    return unsubscribe
  }, [client, streamName])

  return value
}
