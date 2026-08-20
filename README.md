# MiniMax H3 Middleware

面向 MiniMax H3 本地推理的 ComfyUI 中间层。它是 `i2va_middleware/` 的独立重写，旧目录和旧服务不会被修改。

新服务默认监听 `8191`，提供四种原生 H3 生成模式、持久化任务队列、多 ComfyUI 调度与故障转移、任务原子取消、API Key 管理和运维后台。

## 结构

```text
客户端/API Key
      |
      v
HTTP API ---- 管理后台
      |           |
      +----- SQLite（任务、顺序、上游、Key、事件）
                    |
                    v
              中间件调度器
               /    |    \
          ComfyUI  ComfyUI  ComfyUI
```

任务先写入中间件队列，只有上游健康且有空闲容量时才进入 ComfyUI。输入文件保留在中间件本地，选中上游后才上传，因此提交失败可以切换到另一个上游。

## 生成模式

| mode | 输入 | H3 节点 |
| --- | --- | --- |
| `t2va` / `t2v` | 仅 prompt | `MiniMaxH3ImageToVideo`，不接关键帧 |
| `i2va` / `i2v` | 首帧 | `MiniMaxH3ImageToVideo` |
| `fl2va` / `fl2v` | 首帧和尾帧 | `MiniMaxH3ImageToVideo` |
| `ref2va` / `ref2v` | 参考图片、视频、配对音轨或独立音频 | `MiniMaxH3ReferenceToVideo` |

`mode=auto` 会按输入选择 Ref2VA、FL2VA、I2VA；没有媒体时选择 T2VA。兼容入口 `/v1/i2va` 保留旧行为：缺少首帧返回 `400`，不会自动改成 T2VA。

Ref2VA 支持最多 9 张图片、3 个视频、每个视频一个同序号音轨，以及 3 个独立音频。视频会通过原生 `LoadVideo` 和 `GetVideoComponents` 转换为 H3 引用帧；没有显式配对音轨时默认使用视频内嵌音频。

## 启动

本项目只使用 ComfyUI 已有的 `aiohttp`，没有新增运行时依赖。

```bash
cd /root/ComfyUI
export H3_ADMIN_TOKEN='replace-with-a-long-random-admin-token'
export H3_API_TOKEN='replace-with-a-long-random-api-token'
export H3_UPSTREAMS='http://127.0.0.1:8188,http://127.0.0.1:8189'
./h3_middleware/run.sh
```

服务地址：

- API：`http://127.0.0.1:8191/v1/schema`
- 管理后台：`http://127.0.0.1:8191/admin`
- 健康检查：`http://127.0.0.1:8191/health`

未设置环境变量时会使用仅供本机开发的 `h3-admin-change-me` 和 `h3-api-change-me`。服务会打印警告；对外监听前必须替换。

### 主要环境变量

| 变量 | 默认值 | 含义 |
| --- | --- | --- |
| `H3_HOST` | `0.0.0.0` | 监听地址 |
| `H3_PORT` | `8191` | 监听端口 |
| `H3_ADMIN_TOKEN` | 开发凭证 | 后台登录与管理员 Bearer Token |
| `H3_API_TOKEN` | 开发凭证 | bootstrap 客户端凭证 |
| `H3_UPSTREAMS` | `http://127.0.0.1:8188` | 首次启动时写入数据库的上游列表 |
| `H3_DATA_DIR` | `h3_middleware/data` | SQLite 与待分发输入文件 |
| `H3_MAX_ATTEMPTS` | `2` | 默认调度尝试次数 |
| `H3_HEALTH_INTERVAL` | `5` | 上游健康检查秒数 |
| `H3_POLL_INTERVAL` | `1` | 任务历史轮询秒数 |
| `H3_CONDITIONING_NODE` | `MiniMaxH3ImageToVideo` | 默认关键帧条件节点 |

初始上游只在数据库中不存在对应 URL 时加入，不会覆盖后台已经修改的配置。

## 鉴权

公开 API 接受：

```text
Authorization: Bearer <token>
X-API-Key: <token>
X-API-Token: <token>
```

bootstrap token 来自 `H3_API_TOKEN`。数据库 API Key 在后台创建，完整值只返回一次，数据库只保存 SHA-256 摘要。普通 Key 只能读取和操作自己提交的任务；管理员可以操作全部任务。

## 提交任务

### T2VA

```bash
curl -sS http://127.0.0.1:8191/v1/generations \
  -H 'Authorization: Bearer <token>' \
  -H 'Content-Type: application/json' \
  -d '{
    "mode": "t2va",
    "prompt": "A continuous cinematic shot over a quiet mountain lake",
    "duration": 5,
    "width": 1344,
    "height": 768,
    "steps": 20,
    "noise_seed": 42
  }'
```

专用兼容入口为 `POST /v1/t2v`。

### I2VA / FL2VA

```bash
curl -sS http://127.0.0.1:8191/v1/generations \
  -H 'Authorization: Bearer <token>' \
  -F 'mode=fl2va' \
  -F 'first_frame=@/path/first.png' \
  -F 'last_frame=@/path/last.png' \
  -F 'prompt=Move smoothly from the first frame to the last frame' \
  -F 'duration=5'
```

旧字段 `image`、`image_base64`、`image_name` 仍作为首帧接受。`POST /v1/i2va` 和 `/v1/i2va/generate` 保持可用。

### Ref2VA

```bash
curl -sS http://127.0.0.1:8191/v1/ref2va \
  -H 'Authorization: Bearer <token>' \
  -F 'ref_image_0=@/path/person.png' \
  -F 'ref_video_0=@/path/motion.mp4' \
  -F 'ref_video_audio_0=@/path/dialog.wav' \
  -F 'ref_audio_0=@/path/music.wav' \
  -F 'prompt=Use <Picture 1>, <Video 1>, <Audio 1>, and <Audio 2> in one shot'
```

JSON 调用可使用 `ref_image_names`、`ref_video_names`、`ref_video_audio_names` 和 `ref_audio_names`，前提是这些文件名在每个可能被选中的上游输入目录都存在。也可使用 `ref_images_base64`、`ref_videos_base64`、`ref_video_audios_base64` 和 `ref_audios_base64` 数组。

### 同步等待

`POST /v1/generations/sync`、`/v1/t2v/generate`、`/v1/i2va/generate` 和 `/v1/ref2va/generate` 会等待终态。`timeout_sec` 到期后返回 `202` 和当前任务，不会取消任务。设置 `return_binary=true` 可在成功时直接返回首个视频。

## 参数控制

内置适配器控制以下工作流参数：

- prompt、duration、length、可选 `length_expression`
- width、height，或 I2VA/FL2VA 的 megapixels、upscale_method、resolution_steps
- noise_seed、sampler_name、scheduler、steps、denoise
- video_vae、audio_vae、fl2va_unet、ref2va_unet、旧别名 `unet_name`
- weight_dtype、clip_name、clip_type、clip_device
- fps、bit_depth、filename_prefix、format、codec
- ref_image_size、use_embedded_video_audio
- shift_video、shift_audio；设置任意一个时插入 `MiniMaxH3SigmaShift`
- `node_overrides`：按节点 ID 覆盖已存在节点的 `inputs`，未知节点会失败
- `priority`、`max_attempts`：中间件调度参数

完整默认值由 `GET /v1/schema` 返回。假如工作流节点结构不同，应新增适配器，不要让客户端用 `node_overrides` 改 `class_type`。

## 中间件队列

查询：

```bash
curl -H 'Authorization: Bearer <token>' http://127.0.0.1:8191/v1/queue
```

置顶、置底或移动到指定任务前后：

```bash
curl -X PATCH http://127.0.0.1:8191/v1/jobs/<job-id>/queue \
  -H 'Authorization: Bearer <token>' -H 'Content-Type: application/json' \
  -d '{"action":"front"}'

curl -X PATCH http://127.0.0.1:8191/v1/jobs/<job-id>/queue \
  -H 'Authorization: Bearer <token>' -H 'Content-Type: application/json' \
  -d '{"action":"before","target_job_id":"<other-job-id>"}'
```

也可提交 `{"priority": 10}`；优先级越大越靠前。同优先级按显式顺序和创建时间排列。

插队只作用于尚未下发的 `queued/retrying` 任务。默认上游 `max_concurrency=1`，使 ComfyUI 层只持有当前任务，中间层掌握全部待执行顺序。已经 `submitted/running` 的任务不能换位，可以取消后重试。把 `max_concurrency` 调大后，已下发到 ComfyUI 的任务不再能由中间层重排。

管理员可在后台暂停或恢复全局分发。暂停期间仍接受新任务。

## 细粒度进度

中间件为每个 ComfyUI 上游建立独立 WebSocket，并将 ComfyUI 的节点进度实时映射到中间件任务。客户端使用原有任务详情，或只查询进度端点：

```bash
curl -H 'Authorization: Bearer <token>' \
  http://127.0.0.1:8191/v1/jobs/<job-id>/progress
```

响应示例：

```json
{
  "ok": true,
  "job_id": "<job-id>",
  "status": "running",
  "progress": {
    "source": "comfy_websocket",
    "phase": "dit_sampling",
    "node": {"id": "10", "type": "SamplerCustomAdvanced", "state": "running"},
    "step": {"value": 8, "max": 20, "percent": 40.0},
    "workflow": {
      "completed_nodes": 9,
      "total_nodes": 17,
      "percent": 55.29,
      "method": "node_weighted"
    },
    "eta_seconds": 42.5,
    "eta_scope": "current_node",
    "updated_at": 1787200000.0
  }
}
```

`phase` 会区分 `model_loading`、`conditioning`、`dit_sampling`、`vae_decoding`、`video_encoding` 等阶段。`step` 是当前节点内部的小进度；仅当节点上报数值进度时才有值。`eta_seconds` 根据当前节点已经完成的步数估算，只代表当前节点，不是整条工作流 ETA。`workflow.percent` 按已完成节点和当前节点进度加权，是展示用估值，不代表各节点耗时相同。

进度会写入 SQLite，服务重启后仍可查询最后一次状态。客户端可按 0.5 至 1 秒轮询；完整任务响应 `GET /v1/jobs/<job-id>` 中也包含相同的 `progress` 字段。

## 取消与结果

```bash
curl -X POST -H 'Authorization: Bearer <token>' \
  http://127.0.0.1:8191/v1/jobs/<job-id>/cancel

curl -H 'Authorization: Bearer <token>' \
  http://127.0.0.1:8191/v1/jobs/<job-id>

curl -H 'Authorization: Bearer <token>' \
  http://127.0.0.1:8191/v1/jobs/<job-id>/video -o output.mp4
```

本地排队任务直接转为 `canceled`。已下发任务优先调用 ComfyUI 的原子 `POST /api/jobs/{prompt_id}/cancel`；旧上游回退到 queue 快照、pending delete 和定向 interrupt。

管理员 API 还暴露每个上游的 prompt/queue 状态、单条或完整 history、单条与批量 cancel、定向与全局 interrupt、pending delete、queue clear、history delete/clear 和 model/memory free，供后台和自动化运维使用。任务提交不提供直通接口，始终经过中间件队列。

## 多上游与故障转移

调度候选必须同时满足：启用、健康、适配器一致、节点能力满足当前模式、占用量小于最大并发。候选按 `占用量 / 权重` 排序，相同分数轮转。

上传或 `POST /prompt` 明确失败时，同一次调度会尝试下一个候选。连接在提交响应前断开时，中间件会先在原上游查询同一 prompt ID；只有确认不存在才继续，降低重复生成风险。

任务已经被上游接受后，如果上游不可达，状态变为 `upstream_unreachable`，不会立即复制到另一台机器，以免两个上游同时生成。上游恢复后继续对账；上游可达但 prompt 长时间既不在队列也无历史时才按 `max_attempts` 重试。模型执行错误默认终止，可通过 `H3_RETRY_EXECUTION_ERRORS=true` 改为重试。

## 管理后台

`/admin` 使用 `H3_ADMIN_TOKEN` 登录，提供：

- 总任务、运行/排队任务和上游健康概览
- 任务搜索、筛选、详情、取消、重试和置顶
- 队列暂停、恢复、置顶、置底与取消
- 上游增删改、权重/并发、适配器选项、健康检测，以及队列、history、中断和显存原子操作
- API Key 创建、启停与删除
- 调度、故障转移和管理事件记录

管理会话使用 `HttpOnly`、`SameSite=Strict` Cookie。跨主机部署应由 HTTPS 反向代理提供 TLS。

## 工作流适配 skill

项目内 skill 位于：

```text
h3_middleware/skills/adapt-h3-workflow/
```

它包含适配器契约、工作流检查脚本和图结构验证脚本。可将该目录安装到 Codex skills 目录，也可以让 agent 直接从项目路径使用 `$adapt-h3-workflow`。

## 测试

```bash
cd /root/ComfyUI
.venv/bin/python -m unittest discover -s h3_middleware/tests -v
node --check h3_middleware/static/app.js

.venv/bin/python /root/.codex/skills/.system/skill-creator/scripts/quick_validate.py \
  h3_middleware/skills/adapt-h3-workflow
```

测试套件使用假的 ComfyUI HTTP 上游，不会加载模型或产生真实生成任务。

## 目录

```text
h3_middleware/
  assets.py          # 本地输入文件保存与 base64 解析
  comfy_client.py    # ComfyUI 原子 HTTP 操作
  database.py        # SQLite 队列、任务、上游、Key 与事件
  service.py         # 调度、健康检查、故障转移与状态回收
  public_api.py      # 客户端 API 与旧入口
  admin_api.py       # 管理 API
  workflows/         # 可注册的工作流适配器
  static/            # 管理后台
  skills/            # agent 工作流适配 skill
  tests/             # 单元、HTTP 与假上游集成测试
```
