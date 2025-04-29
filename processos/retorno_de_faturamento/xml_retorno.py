def cabecalho(cli, forn, fat, cot, ped):
    return f"""<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:web="http://webservice.logan.cotefacil.com/">
               <soapenv:Header/>
               <soapenv:Body>
                  <web:processarRetornoFaturamentoESBToCTFL>
                     <pedidoFaturadoDTO>
                        <cnpjCliente>{cli}</cnpjCliente>
                        <cnpjFornecedor>{forn}</cnpjFornecedor>
                        <codigoClienteFornecedor></codigoClienteFornecedor>
                        <codigoCondicaoPagamento>V14</codigoCondicaoPagamento>
                        <codigoMotivo>0</codigoMotivo>
                        <codigoPedido></codigoPedido>
                        <dataProcessamento></dataProcessamento>
                        <faturado>{fat}</faturado>
                        <idCotacao>{cot}</idCotacao>
                        <idPedido>{ped}</idPedido>"""


def rodape(motivo):
    return f"""<motivo>{motivo}</motivo>
                <nomeArquivoOrigem></nomeArquivoOrigem>
                <prazoPagamento></prazoPagamento>
                <versaoArquivo>3.1</versaoArquivo>
             </pedidoFaturadoDTO>
          </web:processarRetornoFaturamentoESBToCTFL>
       </soapenv:Body>
    </soapenv:Envelope>"""