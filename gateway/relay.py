"""gateway/relay.py — 双向泵与中继结果分类学 (从 server.py 分层, 2026-11-19)。

pump_streams 是网关热路径: 半关闭语义 + 失败来源分类 (read/write),
产出 'upstream_reset' / 'upstream_eof' / 'client_first' / 'client_error' / 'both_error'。
"""
import asyncio



BUFFER_SIZE = 65536


HALF_CLOSE_GRACE_SECONDS = 5.0


async def pump_streams(r1: asyncio.StreamReader, w1: asyncio.StreamWriter, r2: asyncio.StreamReader, w2: asyncio.StreamWriter) -> str:
    """
    Bidirectionally pumps data between client and upstream proxy.

    Uses half-close semantics: when one direction reaches EOF the peer is signalled
    with write_eof() and the opposite direction gets a bounded grace period to
    finish. Cancelling both directions on first completion truncated HTTP CONNECT
    responses whenever the client half-closed early.

    Returns the relay outcome (2026-11-18 流中级 EOF 反馈, review 二次根修):
    - 'upstream_reset': 上游侧故障 (上游读失败, 或客户端写上游失败) —— 唯一计入
      节点反馈的信号 (线路故障与上游崩溃的公共形态)。
    - 'upstream_eof'  : 上游 clean FIN 先到 —— 不计入。keep-alive 的常规生命周期
      (服务端 idle timeout / Connection: close 响应后关连接), 计入会以每分钟
      数十次的频率误杀健康节点 (review P0 根修)。
    - 'client_first' / 'client_error': 客户端先收尾/中止 —— 不计入 (客户端
      行为; 客户端中止后上游跟着 RST 属正常传播)。
    - 'both_error'    : 双侧异常且来源不可分 —— 不计入。

    2026-11-19 review P2: _forward 现在回报失败来源 ("read"=读对端失败=对端
    故障, "write"=写给对端失败=对端侧管道已死)。旧分类靠"谁先完成"猜根因,
    把「客户端活跃上传时上游死亡」(t1 写半边失败) 误判为 client_error —— 半个
    EOF 闭环一直开着。
    """
    async def _forward(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        try:
            while True:
                data = await reader.read(BUFFER_SIZE)
                if not data:
                    break
                writer.write(data)
                await writer.drain()
            return ("eof", None)
        except (asyncio.IncompleteReadError, ConnectionError, OSError):
            # reader.read 失败 = 我读的对端故障
            return ("error", "read")
        except asyncio.CancelledError:
            raise
        except Exception:
            # 其余 (write/drain 失败) = 我写的对端侧管道已死
            return ("error", "write")
        finally:
            try:
                if writer.can_write_eof():
                    writer.write_eof()
            except (NotImplementedError, RuntimeError, OSError):
                pass

    t1 = asyncio.create_task(_forward(r1, w2))  # 读客户端, 写上游
    t2 = asyncio.create_task(_forward(r2, w1))  # 读上游, 写客户端
    try:
        done, pending = await asyncio.wait({t1, t2}, return_when=asyncio.FIRST_COMPLETED)
        t1_done, t1_res = t1 in done, None
        t2_done, t2_res = t2 in done, None
        # FIRST_COMPLETED 阶段任务不可能被 cancel, result() 安全
        if t1_done:
            t1_res = t1.result()
        if t2_done:
            t2_res = t2.result()
        for task in pending:
            try:
                await asyncio.wait_for(asyncio.shield(task), timeout=HALF_CLOSE_GRACE_SECONDS)
            except asyncio.TimeoutError:
                pass
            except Exception:
                pass
    finally:
        for task in (t1, t2):
            if not task.done():
                task.cancel()
        await asyncio.gather(t1, t2, return_exceptions=True)
        for w in (w1, w2):
            try:
                w.close()
                await w.wait_closed()
            except Exception:
                pass
    # 分类 (2026-11-19 根修): 失败来源取代完成方向。t1 读客户端失败 / t2 写客户端
    # 失败 = 客户端故障; t2 读上游失败 / t1 写上游失败 = 上游故障。客户端 clean
    # close 先到时, 宽限期内的上游传播属正常, 仍优先 client_first。
    if t1_done and t1_res[0] == "eof":
        return "client_first"
    upstream_fault = (
        (t2_done and t2_res[0] == "error" and t2_res[1] == "read")
        or (t1_done and t1_res[0] == "error" and t1_res[1] == "write")
    )
    if upstream_fault:
        return "upstream_reset"
    if t1_done:
        return "client_error"
    if t2_done:
        return "upstream_eof" if t2_res[0] == "eof" else "client_error"
    return "both_error"
