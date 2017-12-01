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
from use_mysql import get_connector, make_tables, register_url
import dbm


necessary_list_dict = dict()   # 接続すべきURLかどうか判断するのに必要なリストをまとめた辞書
after_redirect_list = list()   # リダイレクト後、ジャンプ先ホストとしてあやしくないもの
clamd_q = dict()
machine_learning_q = dict()
screenshots_svc_q = dict()

hostName_process = dict()      # ホスト名 : 子プロセス
hostName_remaining = dict()    # ホスト名 : キューの残り
hostName_pid = dict()          # ホスト名 : pid
hostName_queue = dict()        # ホスト名 : キュー
hostName_achievement = dict()  # ホスト名 : 達成数
hostName_args = dict()         # ホスト名 : 子プロセスの引数   これらホスト名辞書はまとめてもいいが、まとめるとどこで使ってるか分かりにくくなる

pid_time = dict()              # pid : (確認した時間, その時にキューに入っていた最初のURLのタプル)
thread_set = set()             # クローリングするURLかチェックするスレッドのid集合

# notRitsumei_url = set()        # チェックスレッドによって、組織外だと判断されたURL集合
# ritsumei_url = set()           # クローリングすると判断されたURL集合
# black_url = set()              # 組織内だが、クローリングしないと判断されたURL集合
url_db = None                  # 上記３つをまとめたkey-valueデータベース
nth = None                       # 何回目のクローリングか

waiting_list = deque()         # (URL, リンク元)のタプルのリスト(受信したもの全て)
url_list = deque()             # (URL, リンク元)のタプルのリスト(子プロセスに送信用)
assignment_url = set()         # 割り当て済みのURLの集合
remaining = 0
send_num = 0  # 途中経過で表示する5秒ごとの子プロセスに送ったURL数
recv_num = 0  # 途中経過で表示する5秒ごとの子プロセスから受け取ったURL数
all_achievement = 0


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

    # 空文字が入っている場合は削除(急にBLACK_LISTに空文字が入るようになってしまった)
    for v in necessary_list_dict.values():
        if '' in v:
            v.remove('')

    if os.path.exists(path + '/REDIRECT_LIST.txt'):   # 外部サイトへのリダイレクトとして安全が確認されたホスト名のリスト
        data_temp = r_file(path + '/REDIRECT_LIST.txt')
        if data_temp:
            after_redirect_list = data_temp.split('\n')


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
    if not os.path.exists('result/alert'):
        os.mkdir('result/alert')

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
        data_temp = r_json('result_' + str(first_time) + '/all_achievement')   # 総達成数
        all_achievement = data_temp
        data_temp = r_json('result_' + str(first_time) + '/assignment')   # 子プロセスに割り当てたURLの集合
        assignment_url.update(set(data_temp))
        # data_temp = r_json('result_' + str(first_time) + '/searching_url')  # クローリングするURLの集合(割り当てたかどうかは関係ない)
        # ritsumei_url.update(set(data_temp))
        # data_temp = r_json('result_' + str(first_time) + '/unsearching_url')  # クローリングしない(組織外だと判断された)URLの集合
        # notRitsumei_url.update(set(data_temp))
        # data_temp = r_json('result_' + str(first_time) + '/black_url')  # 組織内だがクローリングしないと判断されたURLの集合
        # black_url.update(set(data_temp))
        data_temp = r_json('result_' + str(first_time) + '/url_list')  # これから子プロセスに割り当てるURLの集合
        url_list.extend([tuple(i) for i in data_temp])
        assignment_url.difference_update(set([i[0] for i in url_list]))  # これから割り当てるので割り当て集合から削除する必要(割り当ての際にチェックされるため)
        data_temp = r_json('result_' + str(first_time) + '/waiting_list')  # クローリングするかしないかのチェックをしていないURLの集合
        waiting_list.extend([tuple(i) for i in data_temp])
    # 作業ディレクトリを作って移動
    try:
        os.mkdir('result_' + str(first_time + 1))
        os.chdir('result_' + str(first_time + 1))
    except FileExistsError:
        print('init : result_' + str(first_time + 1) + ' directory has already made.')
        return False
    if clamd_scan:
        # clamdを使うためのプロセスを起動(その子プロセスでclamdを起動)
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


# hostを担当しているプロセスへの通信キューからURLを一つ取り出し、url_listに加える
# url_listに加えないと子プロセスは生成されない(通信キューに残っているだけじゃだめ)
def reborn_child(host):
    try:
        url_tuple = hostName_queue[host]['parent_send'].get(block=False)
    except Exception:
        pass
    else:
        print('main : ' + host + ' is die, although queue is not empty. so append to url_list')
        print('main : add ' + str(url_tuple))
        hostName_remaining[host] -= 1
        url_list.append(url_tuple)
        assignment_url.discard(url_tuple[0])  # 送信URL集合から削除(送信時にチェックしているため)
        return True
    return False


# 残り数が負数を取ることが起き始めたので臨時に調整
# print_progress内で、親から送るキューがemptyの場合、remainingを0にするので、そのあとにreceive()で'receive'を受け取って減算されるとなる？
def adjust_remaining(host):
    url_tuple_list = list()

    # 通信キューから取り出す
    while True:
        try:
            temp = hostName_queue[host]['parent_send'].get(block=False)
        except Exception:
            break
        else:
            url_tuple_list.append(temp)

    # 残り数を実際にカウント
    ret = len(url_tuple_list)
    hostName_remaining[host] = ret

    # 通信キューに戻す
    for url_tuple in url_tuple_list:
        hostName_queue[host]['parent_send'].put(url_tuple)

    return ret


# 5秒ごとに途中経過表示、メインループが動いてることの確認のため、スレッド化していない
def print_progress(run_time_pp, max_process, current_achievement):
    global send_num, recv_num
    alive_count = get_alive_child_num()
    print('main : ---------progress--------')

    count = 0
    alive_count_temp = alive_count
    for host, remaining_temp in hostName_remaining.items():
        if remaining_temp == 0:
            count += 1    # remainingが0のホスト数をカウント
        else:
            if remaining_temp < 0:
                print('main : adjust remaining... , host= ' + host + ' remaining= ' + str(remaining_temp))
                remaining_temp = adjust_remaining(host)
            print('main : ' + host + "'s remaining is " + str(remaining_temp) +
                  '\t active = ' + str(hostName_process[host].is_alive()))
            if hostName_queue[host]['parent_send'].empty():  # 実際にキューの中を見て、空ならばremainingを0に
                hostName_remaining[host] = 0
            else:
                if alive_count_temp < max_process:
                    # プロセスが死んでいて、キューにURLが残っている場合、通信キューから1つ取り出し、url_listに加える
                    if not hostName_process[host].is_alive():
                        if reborn_child(host=host):   # 通信キューからURLをurl_listに追加したら
                            alive_count_temp += 1
    print('main : remaining=0 is ' + str(count))
    print('main : run time = ' + str(run_time_pp) + 's.')
    print('main : URL / recv-num : send-num = ' + str(recv_num) + ' : ' + str(send_num))
    print('main : achievement/assignment = ' + str(current_achievement) + ' / ' + str(len(assignment_url)))
    print('main : all achievement = ' + str(all_achievement + current_achievement))
    print('main : alive child process = ' + str(alive_count))
    print('main : -------------------------')
    send_num = 0
    recv_num = 0


# 強制終了させるために途中経過を保存する
def forced_termination():
    global remaining, all_achievement
    print('main : forced_termination')
    url_list_ft = list()

    # 子に送信する用のキューからURLデータを抜き出し、end()がTrueを返すまで回り続ける
    while end() is not True:
        for queue in hostName_queue.values():
            while True:
                try:
                    url_tuple = queue['parent_send'].get(block=False)  # 子に送信したURLのタプルを全て取り出す
                except Exception:
                    break
                else:
                    url_list_ft.append(url_tuple)   # 続きをする際にもう一度割り振るため、保存しておく
        receive()   # 子プロセスからのデータを抜き取る
        for host_name, host_name_remaining in hostName_remaining.items():
            if host_name_remaining:
                print('main : ' + host_name + ' : remaining --> ' + str(host_name_remaining))
        print('main : alive process : ' + str(get_alive_child_num()))
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
    url_list_ft.extend(url_list)
    w_json(name='url_list', data=url_list_ft)
    w_json(name='assignment', data=list(assignment_url))
    # w_json(name='searching_url', data=list(ritsumei_url))
    # w_json(name='unsearching_url', data=list(notRitsumei_url))
    # w_json(name='black_url', data=list(black_url))
    w_json(name='all_achievement', data=all_achievement)
    w_json(name='waiting_list', data=list(waiting_list))
    remaining = len(url_list_ft) + len(waiting_list)


# 全てのキューに要素がなく、全ての子プロセスが終了していたらTrue
def end():
    if get_alive_child_num():
        # print('main : exist child process.')
        return False
    for host, q_e in hostName_queue.items():  # キューに要素があるかどうか
        if not (q_e['child_send'].empty()):
            print('main : child send queue is not empty.')
            return False
        if not (q_e['parent_send'].empty()):
            return host

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
                # ritsumei_url.add(thread.url_tuple[0])   # 立命館URL集合に追加
                url_db[thread.url_tuple[0]] = 'True,' + str(nth)
                url_list.append((thread.url_tuple[0], thread.url_tuple[1]))    # URLのタプルを検索リストに追加
            elif thread.result == 'black':
                # black_url.add(thread.url_tuple[0])    # 立命館だがblackリストでフィルタリングされたURL集合
                url_db[thread.url_tuple[0]] = 'Black,' + str(nth)
            else:   # (Falseか'unknown')
                # notRitsumei_url.add(thread.url_tuple[0])
                url_db[thread.url_tuple[0]] = 'False,' + str(nth)
                #if thread.result is not False:
                    #wa_file('get_addinfo_e.csv', thread.result)
                # タプルの長さが3の場合はリダイレクト後のURL
                # リダイレクト後であった場合、ホスト名を見てあやしければ外部出力
                if len(thread.url_tuple) == 3:
                    host_name = urlparse(thread.url_tuple[0]).netloc
                    if not [white for white in after_redirect_list if host_name.endswith(white)]:
                        wa_file('../alert/after_redirect_check.csv',
                                thread.url_tuple[0] + ',' + thread.url_tuple[1] + ',' + thread.url_tuple[2] + '\n')
                    else:
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
    t.setDaemon(True)   # メインはスレッドが生きていても死ぬことができる
    try:
        t.start()
    except RuntimeError:
        raise
    else:
        thread_set.add(t)


# urlのホスト名を返す。子プロセス数が上限ならFalseを返す。
def choice_process(url_tuple, max_process, setting_dict, conn, n):
    host_name = urlparse(url_tuple[0]).netloc
    if host_name not in hostName_process:   # まだ作られていない場合、プロセス作成
        # www.ritsumei.ac.jpは子プロセス数が上限でも常に回したい(一番多いから)
        if not host_name == 'www.ritsumei.ac.jp':
            if get_alive_child_num() >= max_process:
                return False

        # 子プロセスと通信するキューを作成
        child_sendq = Queue()
        parent_sendq = Queue()

        # 子プロセスに渡す引数を辞書にまとめる
        args_dic = dict()
        args_dic['host_name'] = host_name
        args_dic['parent_sendq'] = parent_sendq
        args_dic['child_sendq'] = child_sendq
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
        hostName_achievement[host_name] = 0
        hostName_remaining[host_name] = 0
        print('main : ' + host_name + " 's process start. " + 'pid = ' + str(p.pid))
    elif not hostName_process[host_name].is_alive():  # 対象子プロセスが死んでいる場合は再作成
        if not host_name == 'www.ritsumei.ac.jp':
            if get_alive_child_num() >= max_process:
                return False
        print('main : ' + host_name + ' is not alive.')
        # プロセス作成
        p = Process(target=crawler_main, name=host_name, args=(hostName_args[host_name],))
        p.start()   # スタート
        hostName_process[host_name] = p   # プロセスを指す辞書だけ更新する
        print('main : ' + host_name + " 's process start. " + 'pid =' + str(p.pid))
    return host_name


# クローリング子プロセスの中で生きている数を返す
def get_alive_child_num():
    count = 0
    for temp in hostName_process.values():
        if temp.is_alive():
            count += 1
    return count


# 子プロセスからの情報を受信する
# 受信したリストの中のURLはwaiting_list(クローリングするURLかのチェック待ちリスト)に追加する。
def receive():
    # 受信する型は、辞書、タプル、文字列の3種類
    # {'type': '文字列', 'url_tuple_list': [(url, src), (url, src),...]}の辞書
    # (url, 'redirect')のタプル(リダイレクトが起こったが、ホスト名が変わらなかったためそのまま処理された場合)
    # "receive"の文字(子プロセスがURLのタプルを受け取るたびに送信する)
    global recv_num
    for host_name, queue in hostName_queue.items():
        try:
            received_data = queue['child_send'].get(block=False)
        except Exception:   # queueにデータがなければエラーが出る
            continue
        if type(received_data) is str:          # 子プロセスが情報を受け取ったことの確認
            hostName_remaining[host_name] -= 1   # キューに残っているURL数をデクリメント
        elif type(received_data) is tuple:      # リダイレクトしたが、ホスト名が変わらなかったため子プロセスで処理を続行
            assignment_url.add(received_data[0])  # リダイレクト後のURLを割り当てURL集合に追加
            # ritsumei_url.add(received_data[0])    # 立命館URL集合にも追加
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
                    wa_file('../alert/new_window_url.csv', url_tuple[0] + ',' + url_tuple[1] + ',' + url_tuple[2] + '\n')
            elif received_data['type'] == 'redirect':
                url_tuple = received_data['url_tuple_list'][0]   # リダイレクトの場合、リストの要素数は１個だけ
                # if url_tuple[0] in notRitsumei_url:  # 外部組織サーバへのリダイレクトならば
                if url_tuple[0] in url_db:
                    value = url_db[url_tuple[0]].decode('utf-8')
                    value = value[0:value.find(',')]
                    if value == 'False':
                        host_name = urlparse(url_tuple[0]).netloc
                        if not [white for white in after_redirect_list if host_name.endswith(white)]:
                            wa_file('../alert/after_redirect_check.csv',
                                    url_tuple[0] + ',' + url_tuple[1] + ',' + url_tuple[2] + '\n')
                        else:
                            wa_file('after_redirect.csv',
                                    url_tuple[0] + ',' + url_tuple[1] + ',' + url_tuple[2] + '\n')

            # waitingリストに追加。既に割り当て済みの場合は追加しない。
            url_tuple_list = received_data['url_tuple_list']
            if url_tuple_list:
                recv_num += len(url_tuple_list)
                # リンク集から取り出してwaiting_listに追加。
                for url_tuple in url_tuple_list:
                    # if url_tuple[0] in notRitsumei_url:   # 既にチェック済みでクローリングしないURLと分かっているため
                    #     pass
                    # elif url_tuple[0] in black_url:       # ブラックリストに入っているため
                    #     pass
                    # elif url_tuple[0] in ritsumei_url:    # 既に割り当て済みで立命館
                    #     pass
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


# 子プロセスが終了しない、子のメインループも回ってなく、どこかで止まっている場合、親から強制終了させる
# 基準は、親が子に送信する用のキューに同じデータが300秒以上入っているかどうか
def del_child(now):
    for process_dc in hostName_process.values():
        pid_dc = process_dc.pid
        if process_dc.is_alive():
            print('main : alive process : ' + str(process_dc))
            queue = hostName_queue[process_dc.name]
            if pid_dc in pid_time:
                time_dc = pid_time[pid_dc][0]
                if (now - time_dc) > 300:
                    print('main : ' + str(process_dc) + ' over 300 second')
                    if queue['parent_send'].empty():                   # キューが空の場合-----------------------------
                        if pid_time[pid_dc][1] is None:
                            process_dc.terminate()           # キューがずっと空だったので終了させる
                            print('main : ' + str(process_dc) + ' is deleted because it was alive over 300 second')
                            wa_file('notice.txt', str(process_dc) + ' is deleted.\n')
                            del pid_time[pid_dc]
                        else:                     # 現在空だが、登録したときは空じゃなかったため、更新する
                            print('main : ' + str(process_dc) + ' update dictionary')
                            pid_time[pid_dc] = (now, None)
                    else:                                                  # 現在、キューが空じゃない場合----------
                        url_tuple_dc = list()
                        while not(queue['parent_send'].empty()):                    # キューの情報を全て抜き取り
                            try:
                                url_tuple_dc.append(queue['parent_send'].get(block=False))
                            except Exception:
                                break
                        if not url_tuple_dc:
                            pid_time[pid_dc] = (now, None)
                            continue
                        if pid_time[pid_dc][1] == url_tuple_dc[0]:          # 一つ目が同じか比較
                            process_dc.terminate()                          # 同じの場合、プロセスを終了させる
                            print('main : ' + str(process_dc) + ' is deleted due to 300 second. no empty.')
                            wa_file('notice.txt', str(process_dc) + ' is deleted.\n')
                            del pid_time[pid_dc]
                        else:                                              # 違った場合、辞書を更新
                            print('main : ' + str(process_dc) + ' update dictionary')
                            pid_time[pid_dc] = (now, url_tuple_dc[0])
                        for i in url_tuple_dc:                             # 抜き取ったキューは元に戻す
                            queue['parent_send'].put(i)
                else:                                          # 300秒たっていない場合
                    if queue['parent_send'].empty():
                        if not(pid_time[pid_dc][1] is None):
                            pid_time[pid_dc] = (now, None)
                    else:
                        url_tuple_dc = list()
                        try:
                            url_tuple_dc.append(queue['parent_send'].get(block=False))
                        except Exception:
                            pid_time[pid_dc] = (now, None)
                            continue
                        if not(pid_time[pid_dc][1] == url_tuple_dc[0]):
                            pid_time[pid_dc] = (now, url_tuple_dc[0])
                        while not (queue['parent_send'].empty()):
                            try:
                                url_tuple_dc.append(queue['parent_send'].get(block=False))
                            except Exception:
                                break
                        for i in url_tuple_dc:
                            queue['parent_send'].put(i)
            else:
                if queue['parent_send'].empty():
                    pid_time[pid_dc] = (now, None)
                else:
                    url_tuple_dc = list()
                    try:
                        url_tuple_dc.append(queue['parent_send'].get(block=False))
                        pid_time[pid_dc] = (now, url_tuple_dc[0])
                    except Exception:
                        pid_time[pid_dc] = (now, None)
                    while not(queue['parent_send'].empty()):
                        try:
                            url_tuple_dc.append(queue['parent_send'].get(block=False))
                        except Exception:
                            break
                    for i in url_tuple_dc:
                        queue['parent_send'].put(i)
        else:
            try:
                del pid_time[pid_dc]
            except KeyError:
                pass


def crawler_host(n=None):
    global nth
    # spawnで子プロセスを生成するように(windowsではデフォ、unixではforkがデフォ)
    print(get_context())

    # n : 何回目のクローリングか
    if n is None:
        os._exit(255)
    nth = n
    global hostName_achievement, hostName_pid, hostName_process, hostName_queue, hostName_remaining, pid_time
    # global notRitsumei_url, ritsumei_url, black_url
    global waiting_list, url_list, assignment_url, thread_set
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
        hostName_remaining = dict()      # ホスト名 : キューの残り
        hostName_pid = dict()            # ホスト名 : pid
        hostName_queue = dict()          # ホスト名 : キュー
        hostName_achievement = dict()    # ホスト名 : 達成数
        pid_time = dict()                # pid : (確認した時間, その時にキューに入っていた最初のURLのタプル)
        thread_set = set()               # クローリングするURLかチェックするスレッドのid集合
        # notRitsumei_url = set()          # チェックスレッドによって、組織外だと判断されたURL集合
        # ritsumei_url = set()             # クローリングすると判断されたURL集合
        # black_url = set()                # 組織内だが、クローリングしないと判断されたURL集合
        waiting_list = deque()           # (URL, リンク元)のタプルのリスト(クローリングするURLかチェック待ちのリスト)
        url_list = deque()               # (URL, リンク元)のタプルのリスト(子プロセスに送信用)
        assignment_url = set()           # 割り当て済みのURLの集合
        all_achievement = 0
        current_start_time = int(time())
        pre_time = current_start_time

        if not init(first_time=run_count, setting_dict=setting_dict):
            os._exit(255)

        # メインループ
        while True:
            current_achievement = get_achievement_amount()
            receive()
            now = int(time())

            # 途中経過表示
            if now - pre_time >= 5:
                del_child(now)
                print_progress(now - current_start_time, max_process, current_achievement)
                pre_time = now

            # 以下、一気に回すと時間がかかるので、途中保存をして止めたい場合
            if max_time:   # 指定時間経過したら
                if now - start >= max_time:
                    forced_termination()
                    break
            if assign_or_achievement:  # 指定数URLを達成したら
                if len(assignment_url) >= max_page:
                    print('num of assignment reached MAX')
                    while not (get_alive_child_num() == 0):
                        sleep(3)
                        for temp in hostName_process.values():
                            if temp.is_alive():
                                print(temp)
                                current_achievement = get_achievement_amount()
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
            if not waiting_list:   # クローリングするURLかどうかのチェックを待っているURLがなくて
                if not url_list:   # 子プロセスに送るためのURLのリストが空で
                    if not thread_set:  # 立命館かどうかのチェックの最中のスレッドがない場合、end()を呼び出す
                        end_flag = end()
                        if end_flag is True:
                            all_achievement += current_achievement
                            w_json(name='assignment', data=list(assignment_url))
                            break
                        elif end_flag is False:
                            continue
                        else:
                            reborn_child(end_flag)  # 親が送信したURLがキューに残っている場合
            else:
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

            # url_list(子プロセスに送るURLのタプルのリスト)が空じゃなければ取り出す
            if not url_list:
                continue
            else:
                url_tuple = url_list.popleft()
            if url_tuple[0] in assignment_url:    # 滅多にないが同じものが送られていることがある気がする
                wa_file('assign.txt', url_tuple[0] + '\n')
                continue

            # URLのホスト名から、それを担当しているプロセスがなければ(死んでいれば)生成。
            host_name = choice_process(url_tuple, max_process, setting_dict, conn, n)
            if host_name is False:
                url_list.append(url_tuple)  # Falseが返ってくると子プロセス数が上限なので、url_listに戻す
                continue

            # 子プロセスにURLのタプルを送信
            q_to_child = hostName_queue[host_name]['parent_send']  # そのサーバを担当しているプロセスに送るキューをゲット
            if not q_to_child.full():
                if mysql:
                    register_url(conn=conn, url=url_tuple[0], n=n)  # データベースにurlを登録する
                q_to_child.put(url_tuple)
                hostName_remaining[host_name] += 1
                assignment_url.add(url_tuple[0])
                send_num += 1
            else:
                print('main : ' + host_name + ' queue is full')
                url_list.append(url_tuple)

        # メインループを抜け、結果表示＆保存
        print('\nmain : -------------result------------------')
        print('main : assignment_url = ' + str(len(assignment_url)))
        print('main : current achievement = ' + str(current_achievement))
        print('main : all achievement = ' + str(all_achievement))
        print('main : number of child-process = ' + str(len(hostName_process)))
        run_time = int(time()) - current_start_time
        print('run time = ' + str(run_time))
        print('remaining = ' + str(remaining))
        wa_file('result.txt', 'assignment_url = ' + str(len(assignment_url)) + '\n' +
                'current achievement = ' + str(current_achievement) + '\n' +
                'all achievement = ' + str(all_achievement) + '\n' +
                'number of child-process = ' + str(len(hostName_process)) + '\n' +
                'run time = ' + str(run_time) + '\n' +
                'remaining = ' + str(remaining) + '\n' +
                'date = ' + str(date.today()) + '\n')

        print('main : save...')   # 途中結果を保存する
        os.mkdir('TEMP')
        copytree('../../RAD/df_dict', 'TEMP/df_dict')
        copytree('../../RAD/tag_data', 'TEMP/tag_data')
        copytree('../../RAD/url_hash_json', 'TEMP/url_hash_json')
        copytree('../../RAD/temp', 'TEMP/temp')
        copytree('../alert', 'TEMP/alert')
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

        url_db.close()
        # メインループをもう一度回すかどうか
        if save:
            print('main : Restart...')
            run_count += 1
            os.chdir('..')
        else:
            print('main : End')
            break
