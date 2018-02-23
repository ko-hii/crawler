from multiprocessing import Process, Queue, cpu_count, get_context
from urllib.parse import urlparse
from collections import deque
from time import time, sleep
from crawler3 import crawler_main
from file_rw import wa_file, r_file, w_json, r_json
from check_searched_url import CheckSearchedUrlThread
from threading import active_count
import os
from datetime import date
# from machine_learning_screenshots import screenshots_learning_main
# from machine_learning_tag import machine_learning_main
from clamd import clamd_main
from shutil import copytree, copyfile
from use_mysql import get_connector, make_tables
import dbm
import pickle
from summarize_alert import summarize_alert_main


necessary_list_dict = dict()   # 接続すべきURLかどうか判断するのに必要なリストをまとめた辞書
after_redirect_list = list()   # リダイレクト後、ジャンプ先ホストとしてあやしくないもの
clamd_q = dict()
machine_learning_q = dict()
screenshots_svc_q = dict()
summarize_alert_q = dict()

# これらホスト名辞書はまとめてもいいが、まとめるとどこで何を使ってるか分かりにくくなる
hostName_process = dict()      # ホスト名 : 子プロセス
hostName_remaining = dict()    # ホスト名 : 待機URLのリスト
hostName_queue = dict()        # ホスト名 : 通信キュー
hostName_achievement = dict()  # ホスト名 : 達成数
hostName_args = dict()         # ホスト名 : 子プロセスの引数
fewest_host = None     # 待機URL数が一番少ないホスト名

hostName_time = dict()         # ホスト名 : (確認した時間, その時にキューに入っていた最初のURLのタプル)
thread_set = set()             # クローリングするURLかチェックするスレッドのid集合

url_db = None                  # key-valueデータベース. URL : (True or False or Black) + , +  nth
nth = None                     # 何回目のクローリングか

waiting_list = deque()         # (URL, リンク元)のタプルのリスト(受信したもの全て)
url_list = deque()             # (URL, リンク元)のタプルのリスト(子プロセスに送信用)
assignment_url_set = set()     # 割り当て済みのURLの集合
remaining = 0  # 途中保存で終わったときの残りURL数
send_num = 0  # 途中経過で表示する5秒ごとの子プロセスに送ったURL数
recv_num = 0  # 途中経過で表示する5秒ごとの子プロセスから受け取ったURL数
all_achievement = 0
cant_del_child_set = set()   # 子プロセスがsave中なので、del_child()で消してほしくないプロセスの集合


# 設定ファイルの読み込み
def get_setting_dict(path):
    setting = dict()
    bool_variable_list = ['assignOrAchievement', 'screenshots', 'clamd_scan', 'machine_learning', 'phantomjs', 'mecab',
                          'mysql', 'screenshots_svc']
    setting_file = r_file(path + '/SETTING.txt')
    setting_line = setting_file.split('\n')
    for line in setting_line:
        if line and not line.startswith('#'):
            variable = line[0:line.find('=')]
            right_side = line[line.find('=')+1:]
            if variable == 'MAX_page':
                try:
                    value = int(right_side)    # 文字(小数点も)をはじく.でも空白ははじかないみたい
                except ValueError:
                    print("main : couldn't import setting file. because of MAX_page.")
                    setting['MAX_page'] = None
                else:
                    setting['MAX_page'] = value
            elif variable == 'MAX_time':
                right_side_split = right_side.split('*')
                value = 1
                try:
                    for i in right_side_split:
                        value *= int(i)
                except ValueError:
                    print("main : couldn't import setting file. because of MAX_time.")
                    setting['MAX_time'] = None
                else:
                    setting['MAX_time'] = value
            elif variable == 'SAVE_time':
                right_side_split = right_side.split('*')
                value = 1
                try:
                    for i in right_side_split:
                        value *= int(i)
                except ValueError:
                    print("main : couldn't import setting file. because of SAVE_time.")
                    setting['SAVE_time'] = None
                else:
                    setting['SAVE_time'] = value
            elif variable == 'run_count':
                try:
                    value = int(right_side)
                except ValueError:
                    print("main : couldn't import setting file. because of run_count.")
                    setting['run_count'] = None
                else:
                    setting['run_count'] = value
            elif variable == 'MAX_process':
                try:
                    value = int(right_side)
                except ValueError:
                    print("main : couldn't import setting file. because of MAX_process.")
                    setting['MAX_process'] = None
                else:
                    if value == 0:
                        setting['MAX_process'] = cpu_count()
                    else:
                        setting['MAX_process'] = value
            elif variable in bool_variable_list:   # True or Falseの2値しか取らない設定はまとめている
                if right_side == 'True':
                    setting[variable] = True
                elif right_side == 'False':
                    setting[variable] = False
                else:
                    print("main : couldn't import setting file. because of " + variable + '.')
                    setting[variable] = None
            else:
                print("main : couldn't import setting file. because of exist extra setting.")
                print("main : what's " + variable)
                setting['extra'] = None
    return setting


# 必要なリストをインポート
def import_file(path):             # 実行でディレクトリは「crawler」
    global after_redirect_list
    if os.path.exists(path + '/DOMAIN.txt'):
        data_temp = r_file(path + '/DOMAIN.txt')
        if data_temp:
            data_temp = data_temp.split('###')[1]
            necessary_list_dict['domain_list'] = data_temp.split('\n')[1:]      # 組織内ドメインリスト
        else:
            necessary_list_dict['domain_list'] = list()
    else:
        necessary_list_dict['domain_list'] = list()
    if os.path.exists(path + '/NOT_DOMAIN.txt'):
        data_temp = r_file(path + '/NOT_DOMAIN.txt')
        if data_temp:
            data_temp = data_temp.split('###')[1]
            necessary_list_dict['not_domain_list'] = data_temp.split('\n')[1:]  # 組織外ドメインリスト
        else:
            necessary_list_dict['not_domain_list'] = list()
    else:
        necessary_list_dict['not_domain_list'] = list()
    if os.path.exists(path + '/BLACK_LIST.txt'):
        data_temp = r_file(path + '/BLACK_LIST.txt')
        if data_temp:
            data_temp = data_temp.split('###')[1]
            necessary_list_dict['black_list'] = data_temp.split('\n')[1:]       # 組織内だが検査しないリスト
        else:
            necessary_list_dict['black_list'] = list()
    else:
        necessary_list_dict['black_list'] = list()
    if os.path.exists(path + '/WHITE_LIST.txt'):
        data_temp = r_file(path + '/WHITE_LIST.txt')
        if data_temp:
            data_temp = data_temp.split('###')[1]
            necessary_list_dict['white_list'] = data_temp.split('\n')[1:]  # 特定URLにおいて接続するリスト(google.siteなど
        else:
            necessary_list_dict['white_list'] = list()
    else:
        necessary_list_dict['white_list'] = list()
    if os.path.exists(path + '/IPAddress_LIST.txt'):     # 接続するIPアドレスのリスト(今はこれに当てはまっていても接続していない)
        data_temp = r_file(path + '/IPAddress_LIST.txt')
        if data_temp:
            necessary_list_dict['IPAddress_list'] = data_temp.split('\n')
        else:
            necessary_list_dict['IPAddress_list'] = list()
    else:
        necessary_list_dict['IPAddress_list'] = list()

    # 空文字が入っている場合は削除(改行で分けているため、空行があれば空文字が入る)
    for v in necessary_list_dict.values():
        if '' in v:
            v.remove('')

    if os.path.exists(path + '/REDIRECT_LIST.txt'):   # 外部サイトへのリダイレクトとして安全が確認されたホスト名のリスト
        data_temp = r_file(path + '/REDIRECT_LIST.txt')
        if data_temp:
            after_redirect_list = data_temp.split('\n')
            # 空文字が入る場合は削除
            if '' in after_redirect_list:
                after_redirect_list.remove('')


# 必要なディレクトリを作成
def make_dir(screenshots):          # 実行ディレクトリは「crawler」
    if not os.path.exists('ROD/url_hash_json'):
        os.mkdir('ROD/url_hash_json')
    if not os.path.exists('ROD/tag_data'):
        os.mkdir('ROD/tag_data')
    if not os.path.exists('ROD/df_dicts'):
        os.mkdir('ROD/df_dicts')

    if not os.path.exists('RAD/df_dict'):
        os.mkdir('RAD/df_dict')
    if not os.path.exists('RAD/temp'):
        os.mkdir('RAD/temp')
    if not os.path.exists('result'):
        os.mkdir('result')
    # if not os.path.exists('result/alert'):
    #     os.mkdir('result/alert')

    if screenshots:
        if not os.path.exists('RAD/screenshots'):
            os.mkdir('RAD/screenshots')


# いろいろと最初の処理
def init(first_time, setting_dict):    # 実行ディレクトリは「result」、最後の方に「result_*」に移動
    global url_db
    # url_dbの作成
    url_db = dbm.open('../RAD/url_db', 'c')

    machine_learning_ = setting_dict['machine_learning']
    clamd_scan = setting_dict['clamd_scan']
    screenshots_svc = setting_dict['screenshots_svc']

    global all_achievement
    # 検索済みURL、検索待ちURLなど、途中保存データを読み込む。一回目の実行の場合は、START_LISTだけ読み込む。
    if first_time == 0:
        data_temp = r_file('../ROD/LIST/START_LIST.txt')
        data_temp = data_temp.split('\n')
        for ini in data_temp:
            waiting_list.append((ini, 'START'))
    else:
        if not os.path.exists('result_' + str(first_time)):
            print('init : result_' + str(first_time) + 'that is the result of previous crawling is not found.')
            return False
        # 総達成数
        data_temp = r_json('result_' + str(first_time) + '/all_achievement')
        all_achievement = data_temp
        # 子プロセスに割り当てたURLの集合
        data_temp = r_json('result_' + str(first_time) + '/assignment_url_set')
        assignment_url_set.update(set(data_temp))
        # クローリングするかしないかのチェックをしていないURLの集合
        data_temp = r_json('result_' + str(first_time) + '/waiting_list')
        waiting_list.extend([tuple(i) for i in data_temp])
        # ホスト名を見て分類する前のリスト
        data_temp = r_json('result_' + str(first_time) + '/url_list')
        url_list.extend([tuple(i) for i in data_temp])
        # 各ホストごとに分類されたURLのリストの辞書
        with open('result_' + str(first_time) + '/host_remaining.pickle', 'rb') as f:
            data_temp = pickle.load(f)
        hostName_remaining.update(data_temp)

    # 作業ディレクトリを作って移動
    try:
        os.mkdir('result_' + str(first_time + 1))
        os.chdir('result_' + str(first_time + 1))
    except FileExistsError:
        print('init : result_' + str(first_time + 1) + ' directory has already made.')
        return False
    # summarize_alertのプロセスを起動
    recvq = Queue()
    sendq = Queue()
    summarize_alert_q['recv'] = recvq  # 子プロセスが受け取る用のキュー
    summarize_alert_q['send'] = sendq  # 子プロセスから送信する用のキュー
    p = Process(target=summarize_alert_main, args=(recvq, sendq, nth))
    p.start()
    summarize_alert_q['process'] = p

    # clamdを使うためのプロセスを起動(その子プロセスでclamdを起動)
    if clamd_scan:
        recvq = Queue()
        sendq = Queue()
        clamd_q['recv'] = recvq   # clamdプロセスが受け取る用のキュー
        clamd_q['send'] = sendq   # clamdプロセスから送信する用のキュー
        p = Process(target=clamd_main, args=(recvq, sendq))
        p.start()
        clamd_q['process'] = p
        if sendq.get(block=True):
            print('main : connect to clamd')   # clamdに接続できたようなら次へ
        else:
            print("main : couldn't connect to clamd")  # できなかったようならFalseを返す
            return False
    if machine_learning_:
        """
        # 機械学習を使うためのプロセスを起動
        recvq = Queue()
        sendq = Queue()
        machine_learning_q['recv'] = recvq
        machine_learning_q['send'] = sendq
        p = Process(target=machine_learning_main, args=(recvq, sendq, '../../ROD/tag_data'))
        p.start()
        machine_learning_q['process'] = p
        print('main : wait for machine learning...')
        print(sendq.get(block=True))   # 学習が終わるのを待つ(数分？)
    if screenshots_svc:
        # 機械学習を使うためのプロセスを起動
        recvq = Queue()
        sendq = Queue()
        screenshots_svc_q['recv'] = recvq
        screenshots_svc_q['send'] = sendq
        p = Process(target=screenshots_learning_main, args=(recvq, sendq, '../../ROD/screenshots'))
        p.start()
        screenshots_svc_q['process'] = p
        print('main : wait for screenshots learning...')
        print(sendq.get(block=True))   # 学習が終わるのを待つ(数分？)
        """
        return False

    return True


# 各子プロセスの達成数を足し合わせて返す
# 達成数 = ページ数(リンク集が返ってきた数) + ファイル数(ファイルの達成通知の数)
def get_achievement_amount():
    achievement = 0
    for achievement_num in hostName_achievement.values():
        achievement += achievement_num
    return achievement


# 5秒ごとに途中経過表示、メインループが動いてることの確認のため、スレッド化していない
def print_progress(run_time_pp, current_achievement):
    global send_num, recv_num
    alive_count = get_alive_child_num()
    print('main : ---------progress--------')

    count = 0
    for host, remaining_list in hostName_remaining.items():
        remaining_num = len(remaining_list['URL_list'])
        if remaining_num == 0:
            count += 1    # URL待機リストが空のホスト数をカウント
        else:
            if host in hostName_process:
                print('main : ' + host + "'s remaining is " + str(remaining_num) +
                      '\t active = ' + str(hostName_process[host].is_alive()))
            else:
                print('main : ' + host + "'s remaining is " + str(remaining_num) + "\t active = isn't made")
    print('main : remaining=0 is ' + str(count))
    print('main : run time = ' + str(run_time_pp) + 's.')
    print('main : recv-URL : send-URL = ' + str(recv_num) + ' : ' + str(send_num))
    print('main : achievement/assignment = ' + str(current_achievement) + ' / ' + str(len(assignment_url_set)))
    print('main : all achievement = ' + str(all_achievement + current_achievement))
    print('main : alive child process = ' + str(alive_count))
    print('main : remaining = ' + str(remaining))
    print('main : -------------------------')
    send_num = 0
    recv_num = 0


# 強制終了させるために途中経過を保存する
def forced_termination():
    global all_achievement
    print('main : forced_termination')

    # 子プロセスがなくなるまで回り続ける
    while get_alive_child_num() > 0:
        receive_and_send(not_send=True)   # 子プロセスからのデータを抜き取る
        for host_name, host_name_remaining in hostName_remaining.items():
            length = len(host_name_remaining['URL_list'])
            if length:
                print('main : ' + host_name + ' : remaining --> ' + str(length))
        print('main : wait for finishing alive process ' + str(get_alive_child_num()))
        del_child(int(time()))
        sleep(3)

    # クローリングをするURLかどうかのチェックをしているスレッドが全て完了するまで回り続ける
    while thread_set:
        make_url_list(int(time()))
        print('main : wait ritsumeikan check thread...')
        print('main : remaining ' + str(len(thread_set)))
        sleep(3)

    # 続きをするために途中経過を保存する
    all_achievement += get_achievement_amount()
    w_json(name='url_list', data=list(url_list))
    w_json(name='assignment_url_set', data=list(assignment_url_set))
    w_json(name='all_achievement', data=all_achievement)
    w_json(name='waiting_list', data=list(waiting_list))
    with open('host_remaining.pickle', 'wb') as f:
        pickle.dump(hostName_remaining, f)


# 全ての待機キューにURLがなく、全ての子プロセスが終了していたらTrue
def end():
    if waiting_list:            # クローリングするURLかどうかのチェックを待っているURLがなくて
        return False
    if url_list:                # 子プロセスに送るためのURLのリストが空で
        return False
    if thread_set:              # 立命館かどうかのチェックの最中のスレッドがなくて
        return False
    if get_alive_child_num():   # 生きている子プロセスがいなくて
        return False
    for host, remaining_temp in hostName_remaining.items():  # キューに要素があるかどうか
        if len(remaining_temp['URL_list']):
            return False
    return True


# checkスレッド後が終わったURLのタプルを、子プロセスに送るためのリストに追加する
# 外部リンクでリダイレクト後だった場合、ファイルに出力する
# url_listを更新後、チェックを終えたスレッドidはスレッド集合から削除する
# もしくは、300秒以上たってもスレッドが終わらない場合もスレッド集合から削除する
def make_url_list(now_time):
    del_list = list()
    for thread in thread_set:
        if type(thread.result) is not int:     # そのスレッドが最後まで実行されたか
            if thread.result is True:
                url_db[thread.url_tuple[0]] = 'True,' + str(nth)               # 立命館URL
                url_list.append((thread.url_tuple[0], thread.url_tuple[1]))    # URLのタプルを検索リストに追加
            elif thread.result == 'black':
                url_db[thread.url_tuple[0]] = 'Black,' + str(nth)  # 対象URLだがblackリストでフィルタリングされたURL
            else:   # (Falseか'unknown')
                url_db[thread.url_tuple[0]] = 'False,' + str(nth)
                # タプルの長さが3の場合はリダイレクト後のURL
                if len(thread.url_tuple) == 3:
                    w_alert_flag = True
                    redirect_host = urlparse(thread.url_tuple[0]).netloc
                    redirect_path = urlparse(thread.url_tuple[0]).path
                    # リダイレクト後であった場合、ホスト名を見てあやしければ外部出力
                    # if not [white for white in after_redirect_list if redirect_host.endswith(white)]:
                    # ホスト名+パスの途中 までを見ることにしたので、上記の一行では判断できなくなった
                    for white_redirect_url in after_redirect_list:
                        if '+' in white_redirect_url:
                            white_host = white_redirect_url[0:white_redirect_url.find('+')]
                            white_path = white_redirect_url[white_redirect_url.find('+')+1:]
                        else:
                            white_host = white_redirect_url
                            white_path = ''
                        if redirect_host.endswith(white_host) and redirect_path.startswith(white_path):
                            w_alert_flag = False
                    if w_alert_flag:
                        data_temp = dict()
                        data_temp['url'] = thread.url_tuple[0]
                        data_temp['src'] = thread.url_tuple[1]
                        data_temp['file_name'] = 'after_redirect_check.csv'
                        data_temp['content'] = thread.url_tuple[2] + ',' + thread.url_tuple[1] + ',' + thread.url_tuple[0]
                        data_temp['label'] = 'URL,SOURCE,REDIRECT_URL'
                        summarize_alert_q['recv'].put(data_temp)
                        # wa_file('../alert/after_redirect_check.csv',
                        #         thread.url_tuple[0] + ',' + thread.url_tuple[1] + ',' + thread.url_tuple[2] + '\n')
                    # 一応すべて外部出力
                    wa_file('after_redirect.csv',
                            thread.url_tuple[0] + ',' + thread.url_tuple[1] + ',' + thread.url_tuple[2] + '\n')
            del_list.append(thread)
            thread.lock.release()    # スレッドは最後にロックをして待っているのでリリースして終わらせる
        else:
            if now_time - thread.result > 300:    # 300秒経っても終わらない場合は削除
                wa_file('cant_done_check_thread.csv', thread.url_tuple[0] + ',' + thread.url_tuple[1] + '\n')
                del_list.append(thread)
                thread.lock.release()   # スレッドは最初にロックをしているのでリリースしておく
    for thread in del_list:
        thread_set.remove(thread)


# クローリング対象のURLかどうかのチェックスレッドを起動する
def thread_start(url_tuple):
    t = CheckSearchedUrlThread(url_tuple, int(time()), necessary_list_dict,)
    t.setDaemon(True)   # daemonにすることで、メインスレッドはこのスレッドが生きていても死ぬことができる
    try:
        t.start()
    except RuntimeError:
        raise
    else:
        thread_set.add(t)


# クローリングプロセスを生成する、既に一度作ったことがある場合は、プロセスだけ作る
def make_process(host_name, setting_dict, conn, n):
    if host_name not in hostName_process:   # まだ作られていない場合、プロセス作成
        # 子プロセスと通信するキューを作成
        child_sendq = Queue()
        parent_sendq = Queue()

        # 子プロセスに渡す引数を辞書にまとめる
        args_dic = dict()
        args_dic['host_name'] = host_name
        args_dic['parent_sendq'] = parent_sendq
        args_dic['child_sendq'] = child_sendq
        args_dic['alert_process_q'] = summarize_alert_q['recv']
        if setting_dict['clamd_scan']:
            args_dic['clamd_q'] = clamd_q['recv']
        else:
            args_dic['clamd_q'] = False
        if setting_dict['machine_learning']:
            args_dic['machine_learning_q'] = machine_learning_q['recv']
        else:
            args_dic['machine_learning_q'] = False
        if setting_dict['screenshots_svc']:
            args_dic['screenshots_svc_q'] = screenshots_svc_q['recv']
        else:
            args_dic['screenshots_svc_q'] = False
        if setting_dict['mysql']:
            args_dic['mysql'] = {'conn': conn, 'n': str(n)}
        else:
            args_dic['mysql'] = False
        args_dic['phantomjs'] = setting_dict['phantomjs']
        args_dic['mecab'] = setting_dict['mecab']
        args_dic['screenshots'] = setting_dict['screenshots']
        hostName_args[host_name] = args_dic    # クローラプロセスの引数は、サーバ毎に毎回同じなので保存しておく

        # プロセス作成
        p = Process(target=crawler_main, name=host_name, args=(hostName_args[host_name],))
        p.start()    # スタート

        # いろいろ保存
        hostName_process[host_name] = p
        hostName_queue[host_name] = {'child_send': child_sendq, 'parent_send': parent_sendq}
        if host_name not in hostName_achievement:
            hostName_achievement[host_name] = 0
        print('main : ' + host_name + " 's process start. " + 'pid = ' + str(p.pid))
    else:
        del hostName_process[host_name]
        print('main : ' + host_name + ' is not alive.')
        # プロセス作成
        p = Process(target=crawler_main, name=host_name, args=(hostName_args[host_name],))
        p.start()   # スタート
        hostName_process[host_name] = p   # プロセスを指す辞書だけ更新する
        print('main : ' + host_name + " 's process start. " + 'pid =' + str(p.pid))


# クローリング子プロセスの中で生きている数を返す
def get_alive_child_num():
    count = 0
    for host_temp in hostName_process.values():
        if host_temp.is_alive():
            count += 1
    return count


# 子プロセスからの情報を受信する、plzを受け取るとURLを送信する
# 受信したリストの中のURLはwaiting_list(クローリングするURLかのチェック待ちリスト)に追加する。
def receive_and_send(not_send=False):
    # 受信する型は、辞書、タプル、文字列の3種類
    # {'type': '文字列', 'url_tuple_list': [(url, src), (url, src),...]}の辞書
    # (url, 'redirect')のタプル(リダイレクトが起こったが、ホスト名が変わらなかったためそのまま処理された場合)
    # "receive"の文字(子プロセスがURLのタプルを受け取るたびに送信する)
    # "plz"の文字(子プロセスがURLのタプルを要求)
    global recv_num, send_num
    for host_name, queue in hostName_queue.items():
        try:
            received_data = queue['child_send'].get(block=False)
        except Exception:   # queueにデータがなければエラーが出る
            continue
        if type(received_data) is str:       # 子プロセスが情報を受け取ったことの確認
            if received_data == 'plz':     # URLの送信要求の場合
                if not_send:
                    queue['parent_send'].put('nothing')
                else:
                    if hostName_remaining[host_name]['URL_list']:
                        # クローリングするurlを送信
                        url_tuple = hostName_remaining[host_name]['URL_list'].popleft()
                        hostName_remaining[host_name]['update_flag'] = True
                        if url_tuple[0] not in assignment_url_set:  # 一度送ったURLは送らない
                            assignment_url_set.add(url_tuple[0])
                            queue['parent_send'].put(url_tuple)
                            send_num += 1
                    else:
                        # もうURLが残ってないことを教える
                        queue['parent_send'].put('nothing')
        elif type(received_data) is tuple:      # リダイレクトしたが、ホスト名が変わらなかったため子プロセスで処理を続行
            assignment_url_set.add(received_data[0])  # リダイレクト後のURLを割り当てURL集合に追加
            url_db[received_data[0]] = 'True,' + str(nth)
        elif type(received_data) is dict:
            if received_data['type'] == 'links':
                hostName_achievement[host_name] += 1   # ページクローリング結果なので、検索済み数更新
            elif received_data['type'] == 'file_done':
                hostName_achievement[host_name] += 1   # ファイルクローリング結果なので、検索済み数更新して次へ
                continue
            elif received_data['type'] == 'new_window_url':      # 新しい窓(orタブ)に出たURL(今のところ見つかってない)
                url_tuple_list = received_data['url_tuple_list']
                for url_tuple in url_tuple_list:
                    data_temp = dict()
                    data_temp['url'] = url_tuple[0]
                    data_temp['src'] = url_tuple[1]
                    data_temp['file_name'] = 'new_window_url.csv'
                    data_temp['content'] = url_tuple[0] + ',' + url_tuple[1]
                    data_temp['label'] = 'NEW_WINDOW_URL,URL'
                    summarize_alert_q['recv'].put(data_temp)
                    # wa_file('../alert/new_window_url.csv', url_tuple[0] + ',' + url_tuple[1] + ',' + url_tuple[2] + '\n')
            elif received_data['type'] == 'redirect':
                url_tuple = received_data['url_tuple_list'][0]   # リダイレクトの場合、リストの要素数は１個だけ
                if url_tuple[0] in url_db:
                    value = url_db[url_tuple[0]].decode('utf-8')
                    value = value[0:value.find(',')]
                    if value == 'False':
                        redirect_host = urlparse(url_tuple[0]).netloc
                        if not [white for white in after_redirect_list if redirect_host.endswith(white)]:
                            data_temp = dict()
                            data_temp['url'] = url_tuple[0]
                            data_temp['src'] = url_tuple[1]
                            data_temp['file_name'] = 'after_redirect_check.csv'
                            data_temp['content'] = url_tuple[2] + ',' + url_tuple[1] + ',' + url_tuple[0]
                            data_temp['label'] = 'URL,SOURCE,REDIRECT_URL'
                            summarize_alert_q['recv'].put(data_temp)
                            # wa_file('../alert/after_redirect_check.csv',
                            #         url_tuple[0] + ',' + url_tuple[1] + ',' + url_tuple[2] + '\n')
                        wa_file('after_redirect.csv',
                                url_tuple[2] + ',' + url_tuple[1] + ',' + url_tuple[0] + '\n')

            # waitingリストに追加。既に割り当て済みの場合は追加しない。
            url_tuple_list = received_data['url_tuple_list']
            if url_tuple_list:
                recv_num += len(url_tuple_list)
                # リンク集から取り出してwaiting_listに追加。
                for url_tuple in url_tuple_list:
                    if url_tuple[0] not in url_db:
                        waiting_list.append(url_tuple)    # まだ対象URLかのチェックをしていないのでチェック待ちリストに入れる
                    else:
                        value = url_db[url_tuple[0]].decode('utf-8')
                        flag = value[0:value.find(',')]
                        n = value[value.find(',')+1:]
                        if int(n) != nth:
                            url_db[url_tuple[0]] = flag + ',' + str(nth)
                            if flag == 'True':
                                url_list.append((url_tuple[0], url_tuple[1]))    # 今回はまだ割り当てていないため割り当て待ちリストに入れる
                            elif flag == 'False':
                                waiting_list.append(url_tuple)  # 前回はFalseだが、今回もう一度チェックする(ipアドレスの取得に失敗したものもFalseのため)


def allocate_to_host_remaining(url_tuple):
    host_name = urlparse(url_tuple[0]).netloc
    if host_name not in hostName_remaining:
        hostName_remaining[host_name] = dict()
        hostName_remaining[host_name]['URL_list'] = deque()    # URLの待機リスト
        hostName_remaining[host_name]['update_flag'] = False   # 待機リストからURLが取り出されたらTrue
    hostName_remaining[host_name]['URL_list'].append(url_tuple)


# hostName_processの整理(死んでいるプロセスのインスタンスを削除、queueの削除)
# 子プロセスが終了しない、子のメインループも回ってなく、どこかで止まっている場合、親から強制終了させる
# 基準は、待機キューに同じデータが300秒以上入っているかどうか　としていたが、update_flagを作ったのでそれで判断
# 通信キューにURLを溜めないようにしたので変更
def del_child(now):
    del_process_list = list()
    for host_name, process_dc in hostName_process.items():
        if process_dc.is_alive():
            print('main : alive process : ' + str(process_dc))
            if host_name in hostName_time:
                if (now - hostName_time[host_name]) > 300:   # 300秒経っていた場合
                    # if host_name in cant_del_child_set:
                    #     wa_file('notice.txt', str(process_dc) + " is couldn't deleted.\n")
                    # else:
                    process_dc.terminate()  # 300秒ずっと待機URLリストが変化なかったので終了させる
                    del hostName_time[host_name]
                    print('main : terminate ' + str(process_dc) + ' because it was alive over 300 second')
                    wa_file('notice.txt', str(process_dc) + ' is deleted.\n')
                else:   # 300秒経っていない場合、remainingリストからURLが取り出されていたら、時間を更新
                    if hostName_remaining[host_name]['update_flag']:
                        hostName_time[host_name] = now
                        hostName_remaining[host_name]['update_flag'] = False
            else:   # 時間を登録
                hostName_time[host_name] = now
                hostName_remaining[host_name]['update_flag'] = False
            # 通信路が空だった最後の時間をリセット
            if 'latest_time' in hostName_queue[host_name]:
                del hostName_queue[host_name]['latest_time']
        else:  # プロセスが死んでいれば
            if host_name in hostName_time:
                del hostName_time[host_name]
            # 死んでいて、かつ通信路が空ならば、そのときの時間を保存
            if 'latest_time' not in hostName_queue[host_name]:
                if hostName_queue[host_name]['child_send'].empty() and hostName_queue[host_name]['parent_send'].empty():
                    hostName_queue[host_name]['latest_time'] = now

        # 死んでいたプロセスのインスタンスを削除(資源解放のため)
        # 基準は死んでいるプロセスで、かつ通信路が60秒以上空の場合
        if 'latest_time' in hostName_queue[host_name]:
            if now - hostName_queue[host_name]['latest_time'] > 60:
                del_process_list.append(host_name)
    # メモリ解放
    for host_name in del_process_list:
        del hostName_args[host_name]
        del hostName_process[host_name]
        del hostName_queue[host_name]


def crawler_host(n=None):
    global nth
    # spawnで子プロセスを生成するように(windowsではデフォ、unixではforkがデフォ)
    print(get_context())

    # n : 何回目のクローリングか
    if n is None:
        os._exit(255)
    nth = n
    global hostName_achievement, hostName_process, hostName_queue, hostName_remaining, hostName_time, fewest_host
    global waiting_list, url_list, assignment_url_set, thread_set
    global remaining, send_num, recv_num, all_achievement
    start = int(time())

    # 設定データを読み込み
    setting_dict = get_setting_dict(path='ROD/LIST')
    if None in setting_dict.values():
        print('main : check the SETTING.txt')
        os._exit(255)
    assign_or_achievement = setting_dict['assignOrAchievement']
    max_process = setting_dict['MAX_process']
    max_page = setting_dict['MAX_page']
    max_time = setting_dict['MAX_time']
    save_time = setting_dict['SAVE_time']
    run_count = setting_dict['run_count']
    screenshots = setting_dict['screenshots']
    mysql = setting_dict['mysql']

    # 一回目の実行の場合
    if run_count == 0:
        if os.path.exists('RAD'):
            print('RAD directory exists.')
            print('If this running is at first time, please delete this dire.')
            print('Else, you should check the run_count in SETTING.txt.')
            os._exit(255)
        os.mkdir('RAD')
        make_dir(screenshots)
        copytree('ROD/url_hash_json', 'RAD/url_hash_json')
        copytree('ROD/tag_data', 'RAD/tag_data')
        if os.path.exists('ROD/url_db'):
            copyfile('ROD/url_db', 'RAD/url_db')
        with open('RAD/READ.txt', 'w') as f:
            f.writelines("This directory's files are read and written.\n")
            f.writelines("On the other hand, ROD directory's files are not written, Read only.\n\n")
            f.writelines('------------------------------------\n')
            f.writelines('When crawling is finished, you should overwrite the ROD/...\n')
            f.writelines('tag_data/, url_hash_json/\n')
            f.writelines("... by this directory's ones for next crawling by yourself.\n")
            f.writelines('Then, you move df_dict in this directory to ROD/df_dicts/ to calculate idf_dict.\n')
            f.writelines('After you done these, you may delete this(RAD) directory.\n')
            f.writelines("To calculate idf_dict, you must run 'tf_idf.py'.")

    # 必要なリストを読み込む
    import_file(path='ROD/LIST')

    try:
        os.chdir('result')
    except FileNotFoundError:
        print('You should check the run_count in setting file.')

    # databaseに必要なテーブルを作成、コネクターとカーソルを取得
    if mysql:
        conn = get_connector()
        if not make_tables(conn=conn, n=n):
            print('cannot make tables')
            os._exit(255)
    else:
        conn = None

    # メインループを回すループ(save_timeが設定されていなければ、一周しかしない)
    while True:
        save = False
        remaining = 0
        send_num = 0                     # 途中経過で表示する5秒ごとの子プロセスに送ったURL数
        recv_num = 0                     # 途中経過で表示する5秒ごとの子プロセスから受け取ったURL数
        hostName_process = dict()        # ホスト名 : 子プロセス
        hostName_remaining = dict()      # ホスト名 : 待機URLのキュー
        hostName_queue = dict()          # ホスト名 : キュー
        hostName_achievement = dict()    # ホスト名 : 達成数
        hostName_time = dict()           # ホスト名 : (確認した時間, その時にキューに入っていた最初のURLのタプル)
        fewest_host = None
        thread_set = set()               # クローリングするURLかチェックするスレッドのid集合
        waiting_list = deque()           # (URL, リンク元)のタプルのリスト(クローリングするURLかチェック待ちのリスト)
        url_list = deque()               # (URL, リンク元)のタプルのリスト(子プロセスに送信用)
        assignment_url_set = set()           # 割り当て済みのURLの集合
        all_achievement = 0
        current_start_time = int(time())
        pre_time = current_start_time

        if not init(first_time=run_count, setting_dict=setting_dict):
            os._exit(255)

        # メインループ
        while True:
            current_achievement = get_achievement_amount()
            remaining = len(url_list) + len(waiting_list) + sum([len(dic['URL_list']) for dic in hostName_remaining.values()])
            receive_and_send()
            now = int(time())

            # 途中経過表示
            if now - pre_time >= 5:
                del_child(now)
                print_progress(now - current_start_time, current_achievement)
                pre_time = now

            # 以下、一気に回すと時間がかかるので、途中保存をして止めたい場合
            if max_time:   # 指定時間経過したら
                if now - start >= max_time:
                    forced_termination()
                    break
            if assign_or_achievement:  # 指定数URLを達成したら
                if len(assignment_url_set) >= max_page:
                    print('num of assignment reached MAX')
                    while not (get_alive_child_num() == 0):
                        sleep(3)
                        for temp in hostName_process.values():
                            if temp.is_alive():
                                print(temp)
                    break
            else:   # 指定数URLをアサインしたら
                if (all_achievement + current_achievement) >= max_page:
                    print('num of achievement reached MAX')
                    forced_termination()
                    break

            # 指定した時間経過すると実行結果を全て保存する。が、プログラム本体は終わらず、メインループを再スタートする
            if save_time:
                if now - current_start_time >= save_time:
                    forced_termination()
                    save = True
                    break

            # 本当の終了条件
            if end():
                all_achievement += current_achievement
                w_json(name='assignment_url_set', data=list(assignment_url_set))
                break

            # 組織内URLかチェックする待ちリストからpop
            if waiting_list:
                if active_count() < 2000:
                    url_tuple = waiting_list.popleft()    # クローリングするURLかどうかのチェック待ちリストからpop
                    try:
                        thread_start(url_tuple)        # チェックするスレッドを立ち上げる
                    except RuntimeError:
                        waiting_list.append(url_tuple)   # 失敗したら待ちリストに戻す
                else:
                    print("main : number of thread is over 2000.")
                    sleep(1)

            # クローリングするURLかどうかのチェックが終わったものからurl_listに追加する
            make_url_list(now)

            # url_list(子プロセスに送るURLのタプルのリスト)からURLのタプルを取り出して分類する
            while url_list:
                url_tuple = url_list.popleft()
                # ホスト名ごとに分けられている子プロセスに送信待ちリストに追加
                allocate_to_host_remaining(url_tuple=url_tuple)

            # プロセス数が上限に達していなければ、プロセスを生成する
            num_of_process = max_process - get_alive_child_num()
            if num_of_process > 0:
                # remainingリストの中で一番待機URL数が多い順プロセスを生成する
                tmp_list = sorted(hostName_remaining.items(), reverse=True, key=lambda tup: len(tup[1]['URL_list']))
                tmp_list = [tmp for tmp in tmp_list if tmp[1]['URL_list']]   # 待機が0は消す
                if tmp_list:
                    # 一番待機URLが少ないプロセスを1つ作る
                    fewest = tmp_list[-1][0]
                    if fewest_host is None:
                        make_process(fewest, setting_dict, conn, n)
                        num_of_process -= 1
                        fewest_host = fewest
                    else:
                        if fewest_host in hostName_process:
                            if hostName_process[fewest_host].is_alive():
                                pass
                            else:
                                make_process(fewest, setting_dict, conn, n)
                                num_of_process -= 1
                                fewest_host = fewest
                    # 多い順に作る
                    for host_url_list_tuple in tmp_list:
                        if num_of_process <= 0:
                            break
                        host = host_url_list_tuple[0]
                        if host_url_list_tuple[1]['URL_list']:  # remainingに待機URLがあれば
                            if host in hostName_process:
                                if hostName_process[host].is_alive():
                                    continue   # プロセスが活動中なら、次に多いホストを
                            make_process(host, setting_dict, conn, n)
                            num_of_process -= 1

        # メインループを抜け、結果表示＆保存
        remaining = len(url_list) + len(waiting_list) + sum([len(di['URL_list']) for di in hostName_remaining.values()])
        current_achievement = get_achievement_amount()
        print('\nmain : -------------result------------------')
        print('main : assignment_url_set = ' + str(len(assignment_url_set)))
        print('main : current achievement = ' + str(current_achievement))
        print('main : all achievement = ' + str(all_achievement))
        print('main : number of child-process = ' + str(len(hostName_achievement)))
        run_time = int(time()) - current_start_time
        print('run time = ' + str(run_time))
        print('remaining = ' + str(remaining))
        wa_file('result.txt', 'assignment_url_set = ' + str(len(assignment_url_set)) + '\n' +
                'current achievement = ' + str(current_achievement) + '\n' +
                'all achievement = ' + str(all_achievement) + '\n' +
                'number of child-process = ' + str(len(hostName_achievement)) + '\n' +
                'run time = ' + str(run_time) + '\n' +
                'remaining = ' + str(remaining) + '\n' +
                'date = ' + str(date.today()) + '\n')

        print('main : save...')   # 途中結果を保存する
        copytree('../../RAD', 'TEMP')
        print('main : save done')

        if setting_dict['machine_learning']:
            print('wait for machine learning process')
            machine_learning_q['recv'].put('end')       # 機械学習プロセスに終わりを知らせる
            if not machine_learning_q['process'].join(timeout=60):  # 機械学習プロセスが終わるのを待つ
                machine_learning_q['process'].terminate()
        if setting_dict['screenshots_svc']:
            print('wait for screenshots learning process')
            screenshots_svc_q['recv'].put('end')       # 機械学習プロセスに終わりを知らせる
            if not screenshots_svc_q['process'].join(timeout=60):  # 機械学習プロセスが終わるのを待つ
                screenshots_svc_q['process'].terminate()
        if setting_dict['clamd_scan']:
            print('wait for clamd process')
            clamd_q['recv'].put('end')        # clamdプロセスに終わりを知らせる
            if not clamd_q['process'].join(timeout=60):   # clamdプロセスが終わるのを待つ
                clamd_q['process'].terminate()
        print('wait for summarize alert process')
        summarize_alert_q['recv'].put('end')  # summarize alertプロセスに終わりを知らせる
        if not summarize_alert_q['process'].join(timeout=60):  # summarize alertプロセスが終わるのを待つ
            summarize_alert_q['process'].terminate()

        url_db.close()
        # メインループをもう一度回すかどうか
        if save:
            print('main : Restart...')
            run_count += 1
            os.chdir('..')
        else:
            print('main : End')
            break
