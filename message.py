class Mensagem:
    def __init__(self, comando, key=None, value=None, timestamp=0, endereco_servidor=None):
        self.comando = comando
        self.key = key
        self.value = value
        self.timestamp = timestamp
        self.endereco_servidor = endereco_servidor
