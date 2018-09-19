package br.com.qualidade.monitor.model

case class MonitorObject(bases: List[Base])

case class Base(nome: String)