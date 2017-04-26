
class RSockIO(object):

    __buff_size = 8192

    def __init__(self, sock):
        """Initialized with socket, which is usually accepted by a server socket
        
        :param sock: 
        """
        self.__sock = sock
        self.__buff = [" " * self.__buff_size]
        self.__bufPtr = 0
        self.__unreadCnt = 0

    def readnb(self, n):
        """Read n bytes from buffer
        
        :param n: 
        :return: bytes_count, received_content
        """
        nleft = n
        content_readnb = ""
        while nleft > 0:
            cnt, content_read = self.__read(nleft)
            if cnt == 0:
                self.close_sock()
                break
            else:
                nleft -= cnt
                content_readnb = content_readnb + content_read
        return n - nleft, content_readnb

    def readlineb(self, max_len=8192):
        """Read a line from buffer
        
        :param max_len: 
        :return: bytes_count, line_content
        """
        read_cnt = 0
        content_readlineb = ""
        while read_cnt < max_len - 1:
            cnt, content_read = self.__read(1)
            if cnt == 0:
                self.close_sock()
                break
            else:
                read_cnt += 1
                if content_read == "\n":
                    break
                content_readlineb = content_readlineb + content_read
        return read_cnt, content_readlineb

    def __read(self, n):
        """Read n bytes from buffer, manipulate the buffer directly.
        If the buffer is empty, read from the socket first
        
        :param n: 
        :return: bytes_count, content_read
        """
        while self.__unreadCnt <= 0:
            content_received = self.__sock.recv(self.__buff_size).decode('utf-8')
            content_length = len(content_received)
            if content_length == 0:
                return 0, ""
            else:
                self.__buff[0: content_length] = content_received
                self.__unreadCnt += content_length
                self.__bufPtr = 0
        if self.__unreadCnt < n:
            cnt = self.__unreadCnt
        else:
            cnt = n

        content_read = self.__buff[self.__bufPtr: self.__bufPtr + cnt]
        self.__unreadCnt -= cnt
        self.__bufPtr += cnt
        return cnt, "".join(content_read)

    def sendline(self, content):
        """Send the content with "\n" through the socket
        
        :param content: 
        :return: 
        """
        if isinstance(content, bytes):
            content = content + b"\n"
        elif isinstance(content, (list, dict)):
            content = str(content) + "\n"
            content = content.encode()
        elif isinstance(content, str):
            content = content + "\n"
            content = content.encode()
        self.__sock.send(content)

    def sendn(self, content):
        """Send content through the socket
        
        :param content: 
        :return: 
        """
        if not isinstance(content, bytes):
            content = content.encode()
        self.__sock.send(content)

    def close_sock(self):
        """Close the socket
        
        :return:
         
        """
        self.__sock.close()


