from database.utils import consultar_pmir_db, deletar_pmir_db


def consultar_pmir(idcotacao, oficial=False):
    return consultar_pmir_db(idcotacao, oficial)


def deletar_pmir(idcotacao, oficial=False):
    return deletar_pmir_db(idcotacao, oficial)
