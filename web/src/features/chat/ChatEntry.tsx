import { Navigate } from "react-router-dom";
import { getOrCreateActiveChatSessionId } from "./chatSession";

export function ChatEntry() {
  return (
    <Navigate
      to={`/chat/${encodeURIComponent(getOrCreateActiveChatSessionId())}`}
      replace
    />
  );
}
