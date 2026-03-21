import akshare as ak
import pandas as pd
import schedule
import time
import warnings
from datetime import datetime
from typing import Dict, List, Tuple
import requests

warnings.filterwarnings("ignore")  # 忽略 akshare 的一些警告

# ========== 配置区 ==========
# 基金信息：基金名称 -> {持仓股票代码 -> 持仓比例（%）}
FUNDS = {
    "中欧医疗健康混合A": {
        "603259": 10.11,  # 药明康德
        "600276": 10.08,  # 恒瑞医药
        "300759": 7.64,   # 康龙化成
        "300347": 6.54,   # 泰格医药
        "688506": 6.42,   # 百利天恒
        "002821": 5.63,   # 凯莱英
        "002653": 4.65,   # 海思科
        "002294": 4.87,   # 信立泰
        "002422": 4.16,   # 科伦药业
        "688235": 4.10,   # 百济神州
    },
    "招商中证白酒指数LOF A": {
        "600519": 15.38,  # 贵州茅台
        "600809": 15.11,  # 山西汾酒
        "000858": 14.65,  # 五粮液
        "000568": 14.53,  # 泸州老窖
        "002304": 8.34,   # 洋河股份
        "000596": 5.14,   # 古井贡酒
        "603369": 4.97,   # 今世缘
        "603198": 2.47,   # 迎驾贡酒
        "600702": 2.14,   # 舍得酒业
        "603589": 1.87,   # 口子窖
    },
    "前海开源沪港深核心资源灵活配置混合A": {
        "601899": 9.31,   # 紫金矿业
        "600549": 7.00,   # 厦门钨业
        "000426": 6.30,   # 兴业银锡
        "01818": 6.20,    # 招金矿业（港股）
        "600111": 6.06,   # 北方稀土
        "603799": 5.94,   # 华友钴业
        "000408": 5.82,   # 藏格矿业
        "002240": 5.58,   # 盛新锂能
        "000792": 5.24,   # 盐湖股份
        "001203": 4.85,   # 大中矿业
    }
}
# 股票名称映射（可选，用于推送时显示中文名）
STOCK_NAMES = {
    "603259": "药明康德", "600276": "恒瑞医药", "300759": "康龙化成", "300347": "泰格医药",
    "688506": "百利天恒", "002821": "凯莱英", "002653": "海思科", "002294": "信立泰",
    "002422": "科伦药业", "688235": "百济神州", "600519": "贵州茅台", "600809": "山西汾酒",
    "000858": "五粮液", "000568": "泸州老窖", "002304": "洋河股份", "000596": "古井贡酒",
    "603369": "今世缘", "603198": "迎驾贡酒", "600702": "舍得酒业", "603589": "口子窖",
    "601899": "紫金矿业", "600549": "厦门钨业", "000426": "兴业银锡", "01818": "招金矿业",
    "600111": "北方稀土", "603799": "华友钴业", "000408": "藏格矿业", "002240": "盛新锂能",
    "000792": "盐湖股份", "001203": "大中矿业",
}

# 是否显示详细数据（True：打印每只股票涨跌；False：只打印汇总）
SHOW_DETAIL = True

# ========== 推送配置区 ==========
# Server酱 SendKey（留空则不启用）
SERVERCHAN_KEY = "SCT3203T6Rw6GpKrzdAOjXtDfBZvQ3PR"  # 例如 "SCT123456"

# 企业微信机器人 Webhook URL（留空则不启用）
WECOM_WEBHOOK = ""   # 例如 "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=xxxxx"

# 是否在推送中包含个股明细（True/False）
PUSH_DETAILS = True

# ========== 带重试的数据获取 ==========
def fetch_with_retry(func, max_retries=1, delay=1):
    """通用重试函数"""
    for attempt in range(max_retries):
        try:
            return func()
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout,
                requests.exceptions.ChunkedEncodingError, Exception) as e:
            print(f"第 {attempt+1} 次尝试失败：{e}")
            if attempt == max_retries - 1:
                raise
            time.sleep(delay * (2 ** attempt))  # 指数退避
    return None

# ========== 数据缓存（全局） ==========
_A_DATA = None      # 缓存A股全市场数据
_HK_DATA = None     # 缓存港股全市场数据

def refresh_market_data():
    """刷新市场数据缓存，在每次分析开始时调用"""
    global _A_DATA, _HK_DATA
    print("正在获取A股实时数据...")
    try:
        _A_DATA = fetch_with_retry(lambda: ak.stock_zh_a_spot())
        print(f"A股数据获取成功，共 {len(_A_DATA)} 条记录")
    except Exception as e:
        print(f"A股数据获取失败：{e}")
        _A_DATA = None
    '''
    print("正在获取港股实时数据...")
    try:
        _HK_DATA = fetch_with_retry(lambda: ak.stock_hk_spot_em())
        print(f"港股数据获取成功，共 {len(_HK_DATA)} 条记录")
    except Exception as e:
        print(f"港股数据获取失败：{e}")
        _HK_DATA = None
    '''

# ========== 数据获取函数（使用缓存） ==========
def get_a_stock_quote(code: str) -> Tuple[float, float]:
    """从缓存中获取A股实时行情，返回（最新价，涨跌幅%）"""
    if _A_DATA is None:
        print(f"A股数据未加载，无法获取 {code}")
        return None, None
    try:
        # 确保 code 为字符串
        code_str = str(code)
        # 筛选代码列中以 code_str 结尾的行
        matched = _A_DATA[_A_DATA['代码'].str.endswith(code_str)]
        if len(matched) == 0:
            print(f"未找到A股代码 {code}")
            return None, None
        # 取第一个匹配（理论上唯一）
        row = matched.iloc[0]
        return row['最新价'], row['涨跌幅']
    except Exception as e:
        print(f"处理A股 {code} 数据时出错：{e}")
        return None, None

def get_hk_stock_quote(code: str) -> Tuple[float, float]:
    """从缓存中获取港股实时行情，返回（最新价，涨跌幅%）"""
    if _HK_DATA is None:
        print(f"港股数据未加载，无法获取 {code}")
        return None, None
    try:
        # 港股代码在 '代码' 列中形如 '01818'（不带点）
        row = _HK_DATA[_HK_DATA['代码'] == code]
        if len(row) == 0:
            print(f"未找到港股代码 {code}")
            return None, None
        price = row.iloc[0]['最新价']
        pct = row.iloc[0]['涨跌幅']
        return price, pct
    except Exception as e:
        print(f"处理港股 {code} 数据时出错：{e}")
        return None, None

def get_quote(code: str):
    """根据代码自动判断市场并获取行情"""
    if code.isdigit() and len(code) == 6:
        # A股（6位数字）
        return get_a_stock_quote(code)
    elif code.isdigit() and len(code) == 5:
        # 港股（5位数字）
        return None, None
        return get_hk_stock_quote(code)
    else:
        print(f"未知代码格式：{code}")
        return None, None

# ========== 分析函数 ==========
def analyze_fund(fund_name: str, holdings: Dict[str, float]) -> Dict:
    """分析单只基金，返回预估涨跌幅及其他统计"""
    total_pct = 0.0
    valid_count = 0
    details = []
    for code, weight in holdings.items():
        price, change = get_quote(code)
        if price is not None and change is not None:
            total_pct += change * (weight / 100)  # 加权贡献
            valid_count += 1
            details.append({
                'code': code,
                'name': STOCK_NAMES.get(code, code),
                'weight': weight,
                'change': change
            })
        else:
            details.append({
                'code': code,
                'name': STOCK_NAMES.get(code, code),
                'weight': weight,
                'change': None
            })
    return {
        'fund_name': fund_name,
        'predicted_change': total_pct,
        'valid_stocks': valid_count,
        'total_stocks': len(holdings),
        'details': details
    }

# ========== 推送函数 ==========
def send_to_serverchan(title: str, content: str):
    """通过 Server酱 推送消息"""
    if not SERVERCHAN_KEY:
        return
    url = f"https://sctapi.ftqq.com/{SERVERCHAN_KEY}.send"
    data = {
        "title": title,
        "desp": content
    }
    try:
        resp = requests.post(url, data=data, timeout=10)
        if resp.status_code == 200:
            print("Server酱推送成功")
        else:
            print(f"Server酱推送失败，状态码：{resp.status_code}")
    except Exception as e:
        print(f"Server酱推送异常：{e}")

def send_to_wecom(content: str):
    """通过企业微信机器人推送消息"""
    if not WECOM_WEBHOOK:
        return
    # 企业微信机器人要求消息为 JSON 格式，文本内容不能超过 4096 字节
    # 如果内容过长，可考虑分段或使用 markdown 格式
    payload = {
        "msgtype": "text",
        "text": {
            "content": content
        }
    }
    try:
        resp = requests.post(WECOM_WEBHOOK, json=payload, timeout=10)
        if resp.status_code == 200:
            result = resp.json()
            if result.get('errcode') == 0:
                print("企业微信机器人推送成功")
            else:
                print(f"企业微信机器人推送失败：{result}")
        else:
            print(f"企业微信机器人推送失败，状态码：{resp.status_code}")
    except Exception as e:
        print(f"企业微信机器人推送异常：{e}")



def run_analysis():
    """主执行函数：获取所有基金数据并打印结果"""
    # 每次分析前刷新市场数据缓存（保证数据最新）
    refresh_market_data()

    now = datetime.now()
    print("\n" + "="*60)
    print(f"执行时间：{now.strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*60)
    all_results = []
    for fund_name, holdings in FUNDS.items():
        result = analyze_fund(fund_name, holdings)
        all_results.append(result)
        print(f"\n【{fund_name}】")
        print(f"预估当日涨跌幅：{result['predicted_change']:.2f}%")
        print(f"数据有效股票数：{result['valid_stocks']}/{result['total_stocks']}")

        if SHOW_DETAIL and result['details']:
            print("个股明细：")
            for d in result['details']:
                code = d['code']
                name = d['name']
                weight = d['weight']
                change = d['change'] if d['change'] is not None else "无数据"
                if isinstance(change, float):
                    print(f"  {name}-{code} (权重{weight}%)：涨跌幅 {change:+.2f}%")
                else:
                    print(f"  {name}-{code} (权重{weight}%)：{change}")

        # 简单提示
        pred = result['predicted_change']
        if pred > 1:
            print("→ 趋势：明显上涨")
        elif pred > 0:
            print("→ 趋势：小幅上涨")
        elif pred > -1:
            print("→ 趋势：窄幅震荡")
        else:
            print("→ 趋势：下跌压力")

    print("\n" + "="*60 + "\n")

    # 推送结果
    content = build_push_content(all_results)
    # Server酱推送（支持 Markdown，标题用时间）
    if SERVERCHAN_KEY:
        send_to_serverchan(f"基金行情监控 {now.strftime('%m-%d %H:%M')}", content)
    # 企业微信推送
    if WECOM_WEBHOOK:
        # 注意企业微信文本消息有长度限制，若内容过长可考虑分段发送，这里简单处理
        if len(content) > 4000:
            content = content[:4000] + "\n...（内容过长已截断）"
        send_to_wecom(content)


def build_push_content(results: List[Dict]) -> str:
    """构建推送文本内容"""
    now = datetime.now()
    header = f"【基金行情监控】{now.strftime('%Y-%m-%d %H:%M:%S')}\n"
    lines = [header]

    for r in results:
        fund_name = r['fund_name']
        pred = r['predicted_change']
        # 趋势文字
        if pred > 1:
            trend = "明显上涨"
        elif pred > 0:
            trend = "小幅上涨"
        elif pred > -1:
            trend = "窄幅震荡"
        else:
            trend = "下跌压力"

        lines.append(f"\n📊 {fund_name}")
        lines.append(f"\n预估涨跌幅：{pred:+.2f}% （{trend}）")
        lines.append(f"\n数据有效股票：{r['valid_stocks']}/{r['total_stocks']}")

        if PUSH_DETAILS and r['details']:
            lines.append("\n个股明细：")
            for d in r['details']:
                change = d['change']
                if change is not None:
                    # 显示涨跌幅符号
                    lines.append(f"\n  {d['name']}({d['code']}) 权重{d['weight']}% 涨跌幅{change:+.2f}%")
                else:
                    lines.append(f"\n  {d['name']}({d['code']}) 权重{d['weight']}% 无数据")

    return "\n".join(lines)


# ========== 定时任务 ==========
def schedule_tasks():
    """设置每天 12:00 和 14:30 执行"""
    schedule.every().day.at("12:00").do(run_analysis)
    schedule.every().day.at("14:30").do(run_analysis)
    print("定时任务已启动，每天 12:00 和 14:30 执行")
    print("按 Ctrl+C 停止程序...\n")

    while True:
        schedule.run_pending()
        time.sleep(60)  # 每分钟检查一次

if __name__ == "__main__":
    # 先立即执行一次（用于测试）
    run_analysis()
    # 启动定时任务
    #schedule_tasks()