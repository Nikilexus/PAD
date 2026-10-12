# Objectifs du laboratoire

Source de reference: `Agent mesagerie.pdf`.

## Partie 1 - Socket-uri

| Objectif | Implementation |
| --- | --- |
| Format XML/JSON pentru mesajele transmise | JSON newline-delimited frames in `src/integration_service/protocol.py` |
| Agent de mesagerie intre sender, broker si receiver | TCP and UDP adapters in `src/integration_service/socket_transport.py` |
| Rutare dupa tipul/subiectul mesajului | `Broker.subscribe(event_type, consumer)` and socket `subscribe` action |
| Tratarea mesajelor invalide | `ProtocolError` responses without broker crash |
| Protocol de transport selectabil | `python scripts/run_socket_demo.py` opens a terminal menu for TCP or UDP |
| Expeditor si destinatari selectabili | The socket demo lets the user choose one publisher, `send all messages`, and exact receiver targets |
| Caderi de conexiune controlabile | The socket demo lets the user disconnect exact participants or random 30-50% of selected receivers |
| Cereri concurente | TCP uses `ThreadingTCPServer`; UDP dispatches datagrams to worker threads |
| Stocare tranzienta | In-memory queue and DLQ in `src/integration_service/broker.py` |
| Publisher / Subscriber | Five publishers and ten subscribers configured in `src/api/main.py` |
| Politici de livrare | ACK, NACK, retry, max attempts and DLQ in `Broker` |

## Partie 2 - RPC-uri

| Objectif | Implementation |
| --- | --- |
| Utilizarea unui RPC in loc de socket-uri directe | XML-RPC service in `src/integration_service/rpc_service.py` |
| Reprezentarea datelor | JSON payload passed through RPC methods |
| Validarea datelor | Shared message protocol validation in `src/integration_service/protocol.py` |
| Procesare prin broker | RPC publish calls reuse the same broker, routing, ACK and DLQ behavior |

## Verificare

```bash
python -m unittest discover -s tests -p "test_*.py"
python scripts/run_demo.py
python scripts/run_socket_demo.py
python scripts/run_rpc_demo.py
```
