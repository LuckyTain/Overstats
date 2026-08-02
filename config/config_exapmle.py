from __future__ import annotations

import os

# ======================= Core Service ====================== #
API_HOST = "127.0.0.1"
API_PORT = 18080
USE_STREAM_RESPONSE = True
ENABLE_DATABASE_WRITE = True

# ======================= Dashen Upstream ====================== #
# Configure at least one account.
DASHEN_ACCOUNTS = [
    {
        "name": "name",
        "role_id": 123,
        "token": "123",
    },
]

DASHEN_DTS = 2026
DASHEN_SERVER = 1
DASHEN_ACCOUNT_MAX_REQUESTS_PER_SECOND = 5
DASHEN_ACCOUNT_RATE_LIMIT_WINDOW_SECONDS = 1.0
DASHEN_CLIENT_TYPE = "60"
DASHEN_ORIGIN = "https://act.ds.163.com"
DASHEN_REFERER = "https://act.ds.163.com/"
DASHEN_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36 "
    "app/df_client dfVersion/100111"
)
DASHEN_ACCOUNT_FAILURE_COOLDOWN_SECONDS = 60
DASHEN_MAX_CONCURRENT_REQUESTS = 2
# Main v2 Dashen endpoints accept at most account-pool-size * 4 requests
# (active + queued) by default. Extra requests receive HTTP 429.
DASHEN_MAX_ACCEPTED_REQUESTS = max(len(DASHEN_ACCOUNTS) * 4, 1)

# Optional proxy settings.
DASHEN_INTERNATIONAL_PROXY = ""
DASHEN_NETEASE_PROXIES = [
    None,
    # "http://your-netease-proxy:port",
]

# OW esports PandaScore API key.
#如何获取ow赛事的apikey:访问https://app.pandascore.co/dashboard/main，注册并生成api key，每小时1000次免费调用
OW_ESPORTS_API_KEY = ""

# Optional external OW guess asset pack root.
# 仅存放本地图片/音频等大资源，默认放在 Overstats 项目目录外的相邻文件夹。
# Default location: <repo>/ow_guess_assets (gitignored, optional install).
OW_GUESS_ASSET_ROOT = "ow_guess_assets"

# ======================= Dashen Season ====================== #
# Effective Dashen season = max(DASHEN_CURRENT_SEASON, max(AIEvaluateConfig[*].seasonIdList)).
DASHEN_CURRENT_SEASON = 23
DASHEN_HISTORY_START_SEASON = 15

# ======================= OW Hero Leaderboard ====================== #
OW_HERO_LEADERBOARD_CN_SEASON = 3

# ======================= Match Analysis ====================== #
# OpenAI-compatible base URL, for example:
# - https://api.openai.com/v1
# - https://api.deepseek.com/v1
# - https://generativelanguage.googleapis.com/v1beta/openai
# You can also provide the full /chat/completions endpoint directly.
ANALYSIS_BASE_URL = ""
# Keep provider credentials outside the repository. Rotate any key that was
# previously committed here before setting this environment variable.
ANALYSIS_API_KEY = os.getenv("OVERSTATS_ANALYSIS_API_KEY", "")
# Optional proxy for OpenAI official and Google OpenAI-compatible endpoints.
ANALYSIS_PROXY = ""

# ANALYSIS_GOOGLE_MODEL = "gemini-3.6-flash"
#ANALYSIS_DEEPSEEK_MODEL = "deepseek-chat"
#除谷歌和deepseek以外的模型使用下面配置
ANALYSIS_OPENAI_MODEL = ""
ANALYSIS_RATE_LIMIT_REQUESTS = 20
ANALYSIS_RATE_LIMIT_WINDOW_SECONDS = 60
ANALYSIS_CACHE_TTL_SECONDS = 86400
ANALYSIS_CACHE_VERSION = "v1"


# Optional external patch-note fetch proxy.
PATCH_NOTES_USE_INTERNATIONAL_PROXY = False
PATCH_NOTES_INTERNATIONAL_PROXY = ""

# Only put AI persona/tone here. Updated.
# Task instructions and the JSON schema remain in service.py.
ANALYSIS_PERSONA_PROMPT = """
【核心原则】
请保持绝对客观中立，拒绝阿谀奉承。对双方全体玩家进行同等权重的全局复盘，不设置焦点玩家，也不因用于获取数据的玩家身份改变评价。
""".strip()

# Complete user-editable instructions for structured match analysis. The service
# expands {match_summary}, {player_details}, {carry_index_data},
# {match_stat_facts}, {attribute_scores}, and {hero_knowledge}, then appends the required JSON
# response schema. Hero knowledge is also appended automatically when available.
ANALYSIS_MATCH_PROMPT = """
# 身份
你是一位资深《守望先锋 2》职业赛事数据分析师，兼具教练组和赛事解说经验。

# 任务
逐一判断每位玩家在自身职责中的完成度，以及这些表现如何影响比赛胜负。所有结论必须由提供的数据证明；数据无法证明时，不作推断，也不编造对局事件。

# 分析原则
1. 不要只看伤害、治疗或击杀绝对值。必须结合英雄定位、团队职责、死亡、生存、输出效率、资源转换和团队贡献。
2. 只能进行同职责比较：坦克对坦克、输出对输出、辅助对辅助；禁止拿辅助伤害直接和输出位比较。
3. 高爆发英雄重击杀效率、首杀和死亡控制；持续输出英雄重持续伤害、收割与死亡率；控制英雄重限制和团队价值；辅助重治疗、生存、击杀参与、输出与资源利用；坦克重承伤、空间、开团、击杀参与与生存。
4. 死亡数很重要，但高价值换人、首杀和高击杀效率可以抵消部分负面影响。伤害高不等于有效，治疗高不等于完成职责。
5. 助攻不是所有英雄的核心指标；除非英雄机制与其他数据能证明协同性不足，否则不要机械批评“助攻低”。双方数据接近时直接写“没有明显 Diff”。

# 评分标准
S：全场最佳或明显 Carry，职责完成度极高；A：发挥优秀，同职责表现突出；B：完成职责但未决定比赛；C：同职责明显落后并影响团队；D：关键短板或重要失利原因。

# 文风
专业、犀利、客观、简洁，像职业赛事解说。避免“感觉、应该、可能、大概、还不错、可以”等模糊词。每位玩家必须按比赛面板顺序分析，player_id 必须使用输入数据中的完整 BattleTag。
""".strip()
