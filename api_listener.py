from main_response_quotation import *


def remover_da_fila(lista_atual):
    with io.open('fila.json', 'w', encoding='utf-8') as file:
        lista_atual.pop(0)
        json.dump(lista_atual, file)
        file.close()
    return


def validarSeMadrugada():
    hora = int(logs(tipo=2).split()[1].split()[0].split(':')[0])
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
            while True:
                with io.open('fila.json', 'r', encoding='utf-8') as file:
                    lista_atual = json.load(file)
                    if lista_atual:
                        logs(tipo=1,
                             mensagem=logs(tipo=2) + f': Mensagem encontrada, de um total de {len(lista_atual)}')
                        print(f'Mensagem capturada>>>>\n{lista_atual[0]}')
                        processar(lista_atual[0])
                        remover_da_fila(lista_atual)
                    else:
                        logs(tipo=1, mensagem=logs(tipo=2) + ': Fila vazia')
                        if not validarSeMadrugada():
                            time.sleep(60)
                            pass
                        else:
                            time.sleep(7200)
        except Exception as E:
            logs(tipo=1, mensagem=logs(tipo=2) + f': Erro: {E}')
            contador += 1


listener()
