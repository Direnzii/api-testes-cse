import os
import boto3
from main_response_quotation import *

queue_name = 'api-teste-responder-cotacao.fifo'


def remover_da_fila(lista_atual):
    with io.open('arquivos_config/fila.json', 'w', encoding='utf-8') as file:
        lista_atual.pop(0)
        json.dump(lista_atual, file)


def validarSeMadrugada():
    hora = int(logs(tipo=2, amazon=True).split()[1].split()[0].split(':')[0])
    if hora in range(0, 10) or hora in range(21, 23):
        return True
    else:
        return False


def listener():
    contador = 0
    logs(tipo=1, mensagem=logs(tipo=2) + ': Rodando listener >>>')
    while True:
        if contador >= 500:
            logs(tipo=1, mensagem=logs(tipo=2) + ': Quantidade maxima de erros ultrapassada, finalizando processo >>>')
            break
        try:
            session = boto3.session.Session()
            sqs = session.resource('sqs',
                                   aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
                                   aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
                                   region_name=os.getenv('AWS_DEFAULT_REGION'))
            queue_geral = sqs.get_queue_by_name(QueueName=os.getenv('QUEUE_BCKT_NAME_RESPONDER_COTACAO'))
            for message in queue_geral.receive_messages():
                print(message.body)
                print(f'Mensagem capturada>>>>\n{message.body}')
                processar(message.body)
                # deletar mensagem da fila
                message.delete()
        except Exception as E:
            logs(tipo=1, mensagem=logs(tipo=2) + f': Erro: {E}')
            contador += 1


def main():
    listener()


if __name__ == '__main__':
    main()
