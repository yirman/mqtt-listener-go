# Prueba mqtt

Una pequeña aplicación en Go que obtiene mensajes de un broker MQTT y los reenvía.

Diagrama que ilustra el flujo de datos desde el dispositivo GPS del autobús, a través del broker MQTT, hacia el backend y la aplicación pasajero.

![Diagrama de arquitectura](./Diagrama%20MQTT.png)


1) Coordenada y velocidad se envía desde bus transitando a servidor MQTT.
2) Servidor MQTT redirige coordenadas y velocidad a servidor de backend.
3) Backend consulta datos de ETA a un api de google contando con coordenadas
de origen(bus) y destino(siguiente parada tal vez y además destino (universidad ejemplo)?)
4) Api de google retorna datos de ETA.
5) Backend redirige datos de coordenada de bus, velocidad y ETA a servidor MQTT.
6) Servidor MQTT redirige datos de coordenada de bus, velocidad y ETA a pasajeros.