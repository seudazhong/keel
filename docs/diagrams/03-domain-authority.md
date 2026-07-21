# Target domain and authority

```mermaid
flowchart TB
    ORG["Organization"]
    USER["Global User"]
    MEMBERSHIP["Membership"]
    MACHINE["Machine actor"]
    ACCESS["Agent Access<br/>discover / use / manage"]
    AGENT["Agent<br/>personal or team"]
    OWNER["User or Organization owner"]
    CONNECTION["Connection<br/>credential-bearing account"]
    RESOURCE["Selected resource"]
    GRANT["Agent resource grant"]
    ROUTINE["Optional Routine<br/>attenuating policy"]
    SESSION["Session<br/>owner/channel + visibility"]
    RUN["Run<br/>immutable admitted snapshot"]

    USER --> MEMBERSHIP
    MEMBERSHIP --> ORG
    ORG --> AGENT
    USER --> ACCESS
    ACCESS --> AGENT
    OWNER --> CONNECTION
    CONNECTION --> RESOURCE
    RESOURCE --> GRANT
    GRANT --> AGENT
    USER --> SESSION
    AGENT --> SESSION
    AGENT --> RUN
    SESSION --> RUN
    ROUTINE -. narrows authority .-> RUN
    MACHINE --> RUN
```

Effective authority is the intersection of actor membership, Agent Access, Agent grants, Routine
policy, resource state, and deployment capability. `scope_id` is only the derived storage
partition. Agent Access, Connection, Routine, and explicit Session visibility are target entities;
the current implementation contains partial precursors.
