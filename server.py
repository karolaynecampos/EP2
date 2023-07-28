import socket
import threading
import json
import time
from message import Mensagem

class Server:
    def __init__(self):
        self.servidores = [("127.0.0.1", 10097), ("127.0.0.1", 10098), ("127.0.0.1", 10099)]

        # Lê o IP e a porta do servidor
        self.ip = input("Digite o IP do servidor: ")
        self.porta = int(input("Digite a porta do servidor: "))

        # # Lê o IP e a porta do líder
        self.lider_ip = input("Digite o IP do líder: ")
        self.lider_porta = int(input("Digite a porta do líder: "))

        self.endereco_proprio = (self.ip, self.porta)
        self.endereco_lider = (self.lider_ip, self.lider_porta)

        self.lider = (self.endereco_proprio == self.endereco_lider)

        # Tabela de hash local para armazenar as informações
        self.tabela_hash = {}

        # Variável para armazenar as respostas REPLICATION_OK recebidas
        self.respostas_replication_ok = []

        # Inicializa o servidor em uma thread separada
        threading.Thread(target=self.iniciar_servidor).start()

    # Função para lidar com as requisições dos clientes
    def lidar_com_cliente(self, conn, addr):
        with conn:
            while True:
                data = conn.recv(1024)
                if not data:
                    break

                # Desserializa a mensagem JSON recebida
                mensagem_json = data.decode()
                mensagem = json.loads(mensagem_json)

                if mensagem["comando"] == 'PUT':
                    if self.lider:
                        print(f'''Cliente {addr[0]}:{addr[1]} PUT key:{mensagem["key"]} value:{mensagem["value"]}.''')
                        # O servidor é o líder, então processa o PUT localmente
                        timestamp = int(time.time())  # Timestamp atual
                        self.tabela_hash[mensagem["key"]] = (mensagem["value"], timestamp)
                        
                        # Envio da mensagem REPLICATION para os outros servidores
                        self.respostas_replication_ok = []
                        mensagem_replication = Mensagem(comando='REPLICATION', key=mensagem["key"], value=mensagem["value"], timestamp=timestamp)
                        for servidor in self.servidores:
                            if servidor != self.endereco_proprio:
                                resposta = self.enviar_mensagem(mensagem_replication, servidor)
                                self.respostas_replication_ok.append(resposta)

                        # Verifica se todas as respostas REPLICATION_OK foram recebidas
                        if len(self.respostas_replication_ok) == len(self.servidores) - 1:  # Não inclui o próprio servidor
                            # Envia a mensagem PUT_OK para o cliente junto com o timestamp associado à chave
                            chave = mensagem["key"]
                            value, timestamp = self.tabela_hash[chave]
                            resposta = {
                                'comando': 'PUT_OK',
                                'key': chave,
                                'value': value,
                                'timestamp': timestamp,
                                'endereco_servidor': self.endereco_proprio
                            }
                            resposta_json = json.dumps(Mensagem(**resposta).__dict__)
                            conn.sendall(resposta_json.encode())
                    else:
                        print(f'''Encaminhando PUT key:{mensagem["key"]} value:{mensagem["value"]}.''')
                        # Encaminha a requisição PUT para o líder
                        resposta = self.enviar_mensagem(Mensagem(**mensagem), self.endereco_lider)
                        resposta_json = json.dumps(Mensagem(**resposta).__dict__)
                        conn.sendall(resposta_json.encode())

                elif mensagem["comando"] == 'REPLICATION':
                    print(f'''REPLICATION key:{mensagem['key']} value:{mensagem['value']} ts:{mensagem['timestamp']}.''')
                    # Insere a informação na tabela local
                    self.tabela_hash[mensagem["key"]] = (mensagem["value"], mensagem["timestamp"])

                    # Responde para o líder a mensagem REPLICATION_OK
                    resposta = {
                        'comando': 'REPLICATION_OK'
                    }
                    resposta_json = json.dumps(Mensagem(**resposta).__dict__)
                    conn.sendall(resposta_json.encode())

                elif mensagem["comando"] == 'GET':
                    # Verifica se a chave existe na tabela local
                    if mensagem["key"] in self.tabela_hash:
                        value, timestamp = self.tabela_hash[mensagem["key"]]

                        # Verifica o timestamp e envia a resposta apropriada
                        if timestamp >= mensagem["timestamp"]:
                            resposta = {
                                'comando': 'GET_OK',
                                'value': value,
                                'timestamp': timestamp,
                                'endereco_servidor': self.endereco_proprio
                            }
                            print(f'''Cliente {addr[0]}:{addr[1]} GET key:{mensagem['key']} ts:{mensagem['timestamp']}. Meu ts é {timestamp}, portanto devolvendo {value}.''')
                        else:
                            resposta = {
                                'comando': 'TRY_OTHER_SERVER_OR_LATER'
                            }
                            print(f'''Cliente {addr[0]}:{addr[1]} GET key:{mensagem['key']} ts:{mensagem['timestamp']}. Meu ts é {timestamp}, portanto devolvendo TRY_OTHER_SERVER_OR_LATER.''')
                    else:
                        resposta = {
                            'comando': 'GET_OK',
                            'value': None,
                            'timestamp': 0,
                            'endereco_servidor': self.endereco_proprio
                        }
                    resposta_json = json.dumps(Mensagem(**resposta).__dict__)
                    conn.sendall(resposta_json.encode())

    # Função para iniciar o servidor
    def iniciar_servidor(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as servidor:
            servidor.bind(self.endereco_proprio)
            servidor.listen()
            print(f"Servidor iniciado. Aguardando requisições dos clientes...")

            while True:
                conn, addr = servidor.accept()
                threading.Thread(target=self.lidar_com_cliente, args=(conn, addr)).start()

    # Função para enviar uma mensagem para outro servidor
    def enviar_mensagem(self, mensagem, servidor_destino):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect(servidor_destino)
            # Serializa o objeto Mensagem para JSON
            mensagem_json = json.dumps(mensagem.__dict__)
            s.sendall(mensagem_json.encode())
            resposta_json = s.recv(1024).decode()
            resposta = json.loads(resposta_json)
        return resposta

servidor = Server()
