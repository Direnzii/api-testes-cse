from cotefacilib.aws.sqs import get_sqs_objects
from main_response_quotation import *

queue_name = 'api-teste-responder-cotacao.fifo'


def remover_da_fila(lista_atual):
    with io.open('../arquivos_config/fila.json', 'w', encoding='utf-8') as file:
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
        if contador >= 50:
            logs(tipo=1, mensagem=logs(tipo=2) + ': Quantidade maxima de erros ultrapassada, finalizando processo >>>')
            break
        try:

            obj = get_sqs_objects(queue_name=queue_name,
                                  objects_number=0)  # eu estava tentando acessar aqui a fila sqs q eu criei na aws
            lista_atual = []
            if lista_atual:
                logs(tipo=1,
                     mensagem=logs(tipo=2) + f': Mensagem encontrada, de um total de {len(lista_atual)}')
                print(f'Mensagem capturada>>>>\n{lista_atual[0]}')
                processar(lista_atual)
                remover_da_fila(lista_atual)
        except Exception as E:
            logs(tipo=1, mensagem=logs(tipo=2) + f': Erro: {E}')
            contador += 1


def main():
    listener()


if __name__ == '__main__':
    main()
