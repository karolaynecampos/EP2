import socket
import random
import json
from message import Mensagem

class Client:
    def __init__(self):
        self.timestamps = {}
        self.menu()

    def initialize(self):
        # Lê IPs e portas dos servidores
        self.servidores = []
        for i in range(3):
            ip = input(f"Digite o IP do servidor {i+1}: ")
            porta = int(input(f"Digite a porta do servidor {i+1}: "))
            self.servidores.append((ip, porta))

    # Função para enviar uma mensagem e receber a resposta do servidor
    def enviar_mensagem(self, mensagem):
        servidor = random.choice(self.servidores)
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect(servidor)
            # Serializa o objeto Mensagem para JSON
            mensagem_json = json.dumps(mensagem.__dict__)
            s.sendall(mensagem_json.encode())
            resposta_json = s.recv(1024).decode()
            resposta = json.loads(resposta_json)
        return resposta
    
    # Função para atualizar o timestamp da chave no cliente
    def atualizar_timestamp(self, key, timestamp):
        self.timestamps[key] = timestamp

    def menu(self):
        while True:
            # Menu interativo
            comando = input("Digite o comando (INIT, PUT ou GET): ").upper()

            if comando == 'INIT':
                self.initialize()
            
            if comando == 'PUT':
                key = input("Digite a chave (key): ")
                value = input("Digite o valor (value): ")

                # Cria a mensagem PUT
                mensagem = Mensagem(comando=comando, key=key, value=value)

                # Envia a mensagem e recebe a resposta do servidor
                resposta = self.enviar_mensagem(mensagem)

                # Exibe o resultado
                if resposta and resposta["comando"] == 'PUT_OK':
                    self.atualizar_timestamp(key, resposta["timestamp"])
                    print(f'''PUT_OK key: {key} value: {value} timestamp: {resposta["timestamp"]} realizada no servidor {resposta["endereco_servidor"]}''')

            elif comando == 'GET':
                key = input("Digite a chave (key) a ser procurada: ")
                # Verifica se a chave já tem um timestamp associado no cliente
                timestamp = self.timestamps.get(key, 0)

                # Cria a mensagem GET com o timestamp associado
                mensagem = Mensagem(comando=comando, key=key, timestamp=timestamp)

                # Envia a mensagem e recebe a resposta do servidor
                resposta = self.enviar_mensagem(mensagem)

                # Exibe o resultado do GET
                if resposta and resposta["comando"] == 'GET_OK':
                    self.atualizar_timestamp(key, resposta["timestamp"])

                    print(f'''GET key: {key} value: {resposta["value"]} obtido do servidor {resposta["endereco_servidor"]}, meu timestamp {timestamp} e do servidor {resposta["timestamp"]}''')

client = Client()
