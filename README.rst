telecom-take  
======

.. image:: http://slack.kelproject.com/badge.svg
   :target: http://slack.kelproject.com/

.. image:: https://img.shields.io/travis/kelproject/pykube.svg
   :target: https://travis-ci.org/kelproject/pykube

.. image:: https://img.shields.io/pypi/dm/pykube.svg
   :target:  https://pypi.python.org/pypi/pykube/

.. image:: https://img.shields.io/pypi/v/pykube.svg
   :target:  https://pypi.python.org/pypi/pykube/

.. image:: https://img.shields.io/badge/license-apache-blue.svg
   :target:  https://pypi.python.org/pypi/pykube/

Implantar uma ferramentas de big data em uma Telecom;

.. image:: https://static.wixstatic.com/media/c04e44_2fad459154614c368a9194c436febaa6.jpg
   :target: http://slack.kelproject.com/


Acesse qualquer atributo do objeto Kubernetes::

.. code:: scala

    pod = pykube.Pod.objects(api).filter(namespace="gondor-system").get(name="my-pod")
    pod.obj["spec"]["containers"][0]["image"]

Definição e descricao do problema (1 a 2 páginas)
--------
* Desenvolver solucão para verificar se a quota de internet já acabou. Ùlizando logs de transmissão das antenas, fontes de dados: (csv, logs, xml, json, bin …)
* Volumetria esperada (ex: TB mensal/diário de dados e total)
* aspectos funcionais (ex: capacidade de transformação, de busca, etc…)
* não funcionais (ex: performance, requisitos de segurança, etc…)

.. image:: https://3.bp.blogspot.com/-FKWXDqNtIF0/WJoYe3rE5gI/AAAAAAAAAu0/rUd1pvVMQesbA1_imGf4GGj435aRWCqewCLcB/s1600/Big%2BData.jpg
   :target: http://slack.kelproject.com/

Passando pelo fluxo e arquitetura
--------
* Ferramentas ùlizadas (HDFS, Hive, Hbase, Impala, Hawq…)
* Entrada/ingestão, distribuição, processamento, consulta

Transformações dos dados
--------
* Irão sofrer (payload, mudança de format, joins, split, etc…)

.. image:: http://3.bp.blogspot.com/-qMvgSH89YEU/VGdk5GZOcXI/AAAAAAAAAPs/KD3TAsEvrK4/s1600/etl3.jpg
   :target: http://slack.kelproject.com/

Levantamento de custo e viabilidade
--------
* Perfil de hardware (DataNodes, processadores, memória RAM, sensores)
* planejamento de crescimento / ano (ex: consumo de discos, máquinas)
.. image:: https://mineracaodedados.files.wordpress.com/2014/06/big-data-revolution.jpg
   :target: http://slack.kelproject.com/

Viabilidade orçamentária.
--------
* Jus̀ficar a escolha de uma determinada distribuição (ex: Cloudera, HortonWorks, Pivotal, MapR, Apache)
* critérios que julgar importante (suporte, integração, soluções, visão, performance, custo)
