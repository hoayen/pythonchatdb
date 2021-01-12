# coding=utf-8
import ast
import sys
import socket
import sqlite3
import datetime
import json
import time
import random
import threading


class chatDB:
    boss = True
    def __init__(self, path, createNew=False):

        # You have to implement this method
        
        self.con = sqlite3.connect('test.db', check_same_thread = False)
        
        if createNew == True:
            with self.con:
                 cur = self.con.cursor()
                 if createNew == True:
                    cur.execute("""
                        CREATE TABLE Users(
                        user varchar[15] primary key,
                        password varchar[10],
                        status varchar[3])
                        """)
                        		
                    cur.execute("""
                        CREATE TABLE Msgs(
                        sender varchar[15],
                        receiver varchar[15],
                        time_stamp datetime,
                        read varchar[3],
                        primary key(sender, receiver, time_stamp))
                        """)
                        		
                    cur.execute("""
                        CREATE TABLE Cookie(
                        user varchar[15],
                        cookie varchar[15] primary key,
                        Last_acc datetime)
                        """)
                    print"TAO DATABASE THANH CONG."
    pass          
          
       

    def start(self):
    	threading.Thread(target=self.autoClear,args=()).start()
    	print("mo cookie")
    	pass



    def stop(self):
    	threading.Thread(target=self.autoClear, args=()).close()
    	print('dong cookie')
    	pass



    def autoClear(self):
        """
        Clear inactive cookie. Timeout = 600
		Change status of online users into off
        Should be called in self.start

         Xóa cookie không hoạt động. Thời gian chờ = 600
        Thay đổi trạng thái của người dùng trực tuyến thành tắt
        Nên được gọi trong self.start

        """
        # You have to implement this metho
        with self.con:
            cur = self.con.cursor()
            cur.execute("SELECT last_acc FROM Cookie")
            rows = cur.fetchall()
            for row in rows:
                usr = str(row[1])
                now = datetime.datetime.now().replace(microsecond = 0)
                tim = datetime.datetime.strptime(row[0], '%Y-%m-%d %H:%H:%S')
                delta = (now - tim)
                deltatime = delta.total_seconds()
                if deltatime > timeout:
                    cur.execute("DELETE FROM Cookie WHERE last_acc, user = ?",[str(row[0])])
                    cur.execute("UPDATE Users SET status = 'off' WHERE usr = ?", [usr])
        time.sleep(600)
        return
        pass

    def getOnlineUsers(self):
        result=[]
        with self.con:
            cur = self.con.cursor()
            cur.execute("SELECT user FROM Users WHERE status = 'on")
            rows = cur.fetchall()
            for row in rows:
                result.append(str(row[0]))
        if result == []:
            return 'trong'
        else:
            return result
        pass




    def getAllUsers(self):
        result = []
        with self.con:
            cur = self.con.cursor()
            sql = ("SELECT user FROM Users")
            cur.execute(sql)
            rows = cur.fetchall()
            for row in rows:
                result.append(str(row[0]))
        if result == []:
            return 'trong'
        else:
            return result
        pass

    def getAllMsgs(self, cookie, usr2):
        result = []
        with self.con:
            cur = self.con.cursor()
            cur.execute("SELECT cookie, user FROM Cookie WHERE cookie = ?", [cookie])
            row = cur.fetchone()
            if row == None:
                return 'loi_cookie'
            else:
                usr1 = row[1]
            cur.execute("SELECT user FROM Users WHERE user = ?", [usr2])
            row = cur.fetchone()
            if row == None:
                return 'loi_usr'
            cur.execute("SELECT *FROM Msgs WHERE sender = ? and receiver = ? or sender = ? and receiver = ?",
                        (usr1, usr2, usr3))
            row = cur.fetchall()
            for row in rows:
                result.append(list(row))
            cur.execute("UPDATE Msgs SET read = 'already' WHERE sender = ? and receiver = ?", (usr2, usr1))
            return result

        pass

    def getNewMsgs(self, cookie, frm):
		self.con.execute("UPDATE Msgs set Read='already' WHERE Sender=? and Receiver=?",(frm,receiver[0],))
		list=[]
		s=""
		for i in self.con.execute("SELECT Content, Timestamp FROM Msgs WHERE Sender=? and Receiver=?",(frm,receiver[0],)):
		    s = "'"+i[0]+"', '"+i[1]+"'"
		list.append(s)
		self.t[cookie]=time.time() #luu thoi gian hoat dong cua cookie
		s=""
		for i in list:
			s = s+"["+i+"] "
			s = "["+s+"]"
			return s
		
		pass
    def sendMsg(self, cookie, to, content):
        """Send message with content CONTENT from owner of COOKIE to TO.

        The time will be set to the current time on server.

        Return value:
            + 'invalid_usr': if usr is invalid
            + 'invalid_cook': if cookie is invalid
            + 'success'
            Gửi tin nhắn với nội dung CONTENT từ chủ sở hữu COOKIE tới TO.

        Thời gian sẽ được đặt thành thời gian hiện tại trên máy chủ.

        Giá trị trả lại:
            + 'invalid_usr': nếu usr không hợp lệ
            + 'invalid_cook': nếu cookie không hợp lệ
            + 'thành công'
        """
        # You have to implement this method
        return
        pass

    def register(self, usr, wd):

        if len(usr)<3 or len(usr)>10:
            return 'kiem tra lai.'
        if len(wd)<=0:
            return 'sai_pass'
        with self.con:
            cur = self.con.cursor()
            cur.execute("SELECT*FROM Users")
            for row in cur:
                if row[0] == usr:
                    return 'sai ten dang nhạp hoac mat khau'
        with self.con:
            cur = self.con.cursor()
            cur.execute("INSERT INTO Users VALUES (?,?,?)",(usr,wd, 'off'))
            print"dang ky thanh cong "
            return 'dang ky thanh cong'
        pass

    def login(self, usr, wd):
       
        with self.con:
            cur = self.con.cursor()
            cur.execute("SELECT user, password FROM Users")
            rows = cur.fetchall()
            for row in rows:
                if row[0] == usr:
                    if row[1] != wd:
                        return 'loi_wd'
                    else:
                        cur.execute("UPDATE Users SET status = 'on' WHERE user = ?",[usr])
                        cur.execute("SELECT * FROM Cookie WHERE user = ?", [usr])
                        row = cur.fetchone()
                        if row == None:
                            while True:
                                cook = str(random.randint(1000000000000000,9999999999999999))
                                cur.execute("SELECT cookie, user FROM Cookie WHERE cookie = ?",[cook])
                                row = cur.fetchall()
                                if cook not in row:
                                    break
                            now = datetime.datetime.now().replace(microsecond= 0)
                            cur.execute("INSERT INTO Cookie VALUES (?,?,?)", (cook,usr,now))
                            print"Dang Nhap Thanh Cong 1"
                            return['success', cook]
                        else:
                        	cook = str(row[0])
                        	now = datatime.datatime.now().replace( microsecond = 0)
                        	cur.execute("UPDATE Cookie SET last_acc = ? WHERE cookie = ?", (now, cook))
                        	print"Dang Nhap Thanh Cong 2"
                        	return['success', cook]
        return[False,'dang nhap that bai']
        pass



    def logout(self, cookie):
        """Set owner of cookie as logged out
        Remove cookie from database

        Return:
            'success': log-out successfully
            'invalid': log-out failed. The cookie does not exist.

            Đặt chủ sở hữu của cookie là đã đăng xuất
        Xóa cookie khỏi cơ sở dữ liệu

        Trở về:
            'thành công': đăng xuất thành công
            'không hợp lệ': đăng xuất không thành công. Cookie không tồn tại.
        """
        # You have to implement this method
        # Bạn phải thực hiện phương pháp này
        with self.con:
            cur = self.con.cursor()
            cur.execute("SELECT cookie, user FROM Cookie WHERE cookie = ? ",[cookie])
            row = cur.fetchall()
            if row == None:
            	print"kiem tra lai thong tin vua nhap"
                return 'kiem tra lai thong tin vua nhap'
            else:
                now = datetime.datetime.now().replace(microsecond = 0)
                cur.execute("UPDATE Cookie SET last_acc = ? WHERE cookie = ?", (now, cookie))
                cur.execute("SELECT cookie, user FROM Cookie WHERE cookie = ? ",[cookie])
                row = cur.fetchone()
                usr = row[1]
                cur.execute("UPDATE Users SET status = 'off' WHERE user = ?",[usr])
                print"dang xuat thanh cong"
                return'dang xuat thanh cong'
        pass

    # you can define more method here
    # bạn có thể xác định thêm phương pháp tại đây











class ThreadedServer:

    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind((self.host, self.port))




    def listen(self, maxClient):
        self.sock.listen(maxClient)
        while True:
            client, address = self.sock.accept()
            print("Client"+ str(address) + "ket noi thanh cong\n")
            threading.Thread(target=self.listenToClient,
                             args=(client, address)).start()




    def listenToClient(self, client, address):
        recvBuf = ''
        while True:
            data = self.recvLine(client, recvBuf)
            print data
            if type(data) == list:
                ###
                # log down error, may be into db
                ###
                return(data)
            else:
                # convert string representation of any type to that type
                try:
                    request = ast.literal_eval(data)
                except:
                    continue
                response = self.processRequest(request)
                # convert any type of response to string representation
                data = str(response)
                try:
                    client.send(data + '\n')
                except err:
                    client.close()
                    ###
                    # log down error, may be into db
                    ###
                    return(data)  # data = 'Hello\nHello'



    def recvLine(self, client, recvBuf):  # receive line from client
        while '\n' not in recvBuf:
            try:
                data = client.recv(1024)
                if data:
                    recvBuf += data
                # else:
                #     return [False, 'disconnected']
            except:
                client.close()
                return [False, 'cleint dang xuat.']
        lineEnd = recvBuf.index('\n')
        data = recvBuf[:lineEnd]
        recvBuf = recvBuf[lineEnd + 1:]
        return(data)
#yêu cầu xử lý


    def processRequest(self, request):
        global chatdb


        """Process a request of a client

        A request is in the form:
            ['ONLINE'] => getOnlineUsers
            ['ALL'] => getAllUsers
            ['GET', cookie, usr2] => getAllMsgs
            ['NEW', cookie, frm] => getNewMsgs
            ['SEND', cookie, to, content] => sendMsg
            ['REG', usr, wd] => register
            ['LOGIN', usr, wd] => login
            ['LOGOUT', cookie] => logout
        """
        # You have to implement this method
        if request[0] == "ONLINE":
            return chatdb.getOnlineUsers()
        elif request[0] == "ALL":
            return chatdb.getAllUsers()
        elif request[0] == "GET":
            return chatdb.getAllMsgs(request[1], request[2])
        elif request[0] == "NEW":
            return chatdb.getNewMsgs(request[1], request[2])
        elif request[0] == "SEND":
            return chatdb.sendMsg(request[1], request[2], request[3])
        elif request[0] == "REG":
            return chatdb.register(request[1], request[2])
        elif request[0] == "LOGIN":
            return chatdb.login(request[1], request[2])
        elif request[0] == "LOGOUT":
            return chatdb.logout(request[1])
        else:
            return False

        pass


chatdb = None
if __name__ == "__main__":
    if len(sys.argv) != 4:
        print"Usage: %s <port> <dbFile> <createNew>" % sys.argv[0]
        print"Example: %s 8081 chat.sqlite new" % sys.argv[0]
        exit(1)
    port = int(sys.argv[1])
    dbFile = sys.argv[2]
    createNew = sys.argv[3]
    if createNew == 'new':
        createNew = True
    else:
        createNew = False


    chatdb = chatDB(dbFile, createNew)

    chatdb.start()#run chat db API
    ThreadedServer('', port).listen(50)







