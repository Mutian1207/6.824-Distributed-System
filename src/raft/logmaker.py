import re
import sys
from colorama import Fore, Back, Style, init

# 初始化colorama
init(autoreset=True)

class LogAnalyzer:
    def __init__(self):
        self.current_test = None

    def parse_stdin(self):
        for line in sys.stdin:
            self.parse_line(line.strip())

    def parse_line(self, line):
        # 检测新的测试开始
        test_match = re.search(r'Test \((2A|2B|2C)\): (.+)\.\.\.', line)
        if test_match:
            self.current_test = f"{test_match.group(1)}: {test_match.group(2)}"
            print(f"\n{Fore.YELLOW}{Style.BRIGHT}New Test: {self.current_test}")
            return

        # 选举超时
        timeout_match = re.search(r'Now Raft (\d+) \'s election timeout\|\| starting election', line)
        if timeout_match:
            node = timeout_match.group(1)
            print(f"{Fore.RED}[TIMEOUT] Node {node} started election")

        # 发送投票请求
        send_match = re.search(r'Raft (\d+) is sending vote request to Raft (\d+)', line)
        if send_match:
            sender, receiver = send_match.groups()
            print(f"{Fore.BLUE}[SEND] (Vote Request) Node {sender} -> Node {receiver}")

        # 处理投票请求
        handle_match = re.search(r'Raft (\d+) is handling vote request from Raft (\d+)', line)
        if handle_match:
            handler, candidate = handle_match.groups()
            
            spaces = " " * 50
            print(f"{Fore.GREEN} {spaces} Node {candidate} -> Node {handler} [RECV] (Vote Request)")

        # 成为领导者（如果有这样的日志）
        leader_match = re.search(r'Node (\d+) became Leader', line)
        if leader_match:
            node = leader_match.group(1)
            print(f"{Fore.MAGENTA}[LEADER] Node {node} became Leader")

        # 发送空心跳 
        heartbeat_match = re.search(r'Raft (\d+) is sending empty heartbeat to Raft (\d+)', line)
        if heartbeat_match:
            sender, receiver = heartbeat_match.groups()
            print(f"{Fore.CYAN}[SEND] (Heartbeat) Node {sender} -> Node {receiver}")
            
        # 处理空心跳
        handle_heartbeat_match = re.search(r'Raft (\d+) is handling AppendEntries request from Raft (\d+)', line)
        if handle_heartbeat_match:
            handler, leader = handle_heartbeat_match.groups()
            spaces = " " * 50
            print(f"{Fore.WHITE} {spaces} Node {leader} -> Node {handler} [RECV] (Heartbeat)")
        # 测试结果
        if "PASS" in line or "Pass" in line or "pass" in line:
            print(f"{Fore.GREEN}{Style.BRIGHT}[RESULT] {line}")
        elif "FAIL" in line or "Fail" in line or "fail" in line:
            print(f"{Fore.RED}{Style.BRIGHT}[RESULT] {line}")

        # 警告和错误
        if "warning" in line.lower() or "error" in line.lower():
            print(f"{Fore.YELLOW}{Style.BRIGHT}[WARNING/ERROR] {line}")

if __name__ == "__main__":
    analyzer = LogAnalyzer()
    analyzer.parse_stdin()