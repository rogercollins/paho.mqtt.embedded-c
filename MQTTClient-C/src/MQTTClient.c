/*******************************************************************************
 * Copyright (c) 2014, 2017 IBM Corp.
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * and Eclipse Distribution License v1.0 which accompany this distribution.
 *
 * The Eclipse Public License is available at
 *    http://www.eclipse.org/legal/epl-v10.html
 * and the Eclipse Distribution License is available at
 *   http://www.eclipse.org/org/documents/edl-v10.php.
 *
 * Contributors:
 *   Allan Stockdill-Mander/Ian Craggs - initial API and implementation and/or initial documentation
 *   Ian Craggs - fix for #96 - check rem_len in readPacket
 *   Ian Craggs - add ability to set message handler separately #6
 *******************************************************************************/
#include "MQTTClient.h"

#include <stdio.h>
#include <string.h>

//#define DEBUG 1
#include <debug.h>


static void ConnectEnd(MQTTClient *c);
static void SubscribeEnd(MQTTClient* c);
static void PublishEnd(MQTTClient* c);
static void ClientConnect(MQTTClient* c);
void async_waitfor(MQTTClient* c, int packet_type, void (*fp)(MQTTClient *), int timeout_ms);


static void NewMessageData(MessageData* md, MQTTString* aTopicName, MQTTMessage* aMessage) {
    md->topicName = aTopicName;
    md->message = aMessage;
}


static int getNextPacketId(MQTTClient *c) {
    return c->next_packetid = (c->next_packetid == MAX_PACKET_ID) ? 1 : c->next_packetid + 1;
}


static int sendPacket(MQTTClient* c, int length, Timer* timer)
{
    int rc = FAILURE,
        sent = 0;

    ASSERT(c->busy == 0);
    c->busy = 1;

    while (sent < length && !TimerIsExpired(timer))
    {
        rc = c->ipstack->mqttwrite(c, &c->buf[sent], length, TimerLeftMS(timer));
        if (rc < 0)  // there was an error writing the data
            break;
        sent += rc;
    }
    if (sent == length)
    {
        TimerCountdown(&c->last_sent, c->keepAliveInterval); // record the fact that we have successfully sent the packet
        rc = SUCCESS;
    }
    else
        rc = FAILURE;
    return rc;
}


void MQTTClientInit(MQTTClient* c, Network* network, unsigned int command_timeout_ms,
		unsigned char* sendbuf, size_t sendbuf_size)
{
    int i;
    c->ipstack = network;

    for (i = 0; i < MAX_MESSAGE_HANDLERS; ++i)
        c->messageHandlers[i].topicFilter = 0;
    c->command_timeout_ms = command_timeout_ms;
    c->buf = sendbuf;
    c->buf_size = sendbuf_size;
    c->isconnected = 0;
    c->cleansession = 0;
    c->defaultMessageHandler = NULL;
	  c->next_packetid = 1;
    TimerInit(&c->last_sent);
    TimerInit(&c->last_received);

    c->busy = 0;

    c->stats.cnt = c->stats.max = c->stats.sum = 0;
    c->stats.min = 1000000;

#if defined(MQTT_TASK)
	  MutexInit(&c->mutex);
#endif
}


static int decodePacket(MQTTClient* c, int* value, int timeout)
{
    unsigned char i;
    int multiplier = 1;
    int len = 0;
    const int MAX_NO_OF_REMAINING_LENGTH_BYTES = 4;

    *value = 0;
    do
    {
        int rc = MQTTPACKET_READ_ERROR;

        if (++len > MAX_NO_OF_REMAINING_LENGTH_BYTES)
        {
            rc = MQTTPACKET_READ_ERROR; /* bad data */
            goto exit;
        }
        rc = c->ipstack->mqttread(c, &c->readbuf[len], 1, timeout);
        if (rc != 1)
            goto exit;
        i = c->readbuf[len];
        *value += (i & 127) * multiplier;
        multiplier *= 128;
    } while ((i & 128) != 0);
exit:
    return len;
}


static int readPacket(MQTTClient* c, Timer* timer)
{
    MQTTHeader header = {0};
    int len = 0;
    size_t rem_len = 0;

    /* 1. read the header byte.  This has the packet type in it */
    int rc = c->ipstack->mqttread(c, c->readbuf, 1, TimerLeftMS(timer));
    if (rc != 1)
        goto exit;

    len = 1;
    /* 2. read the remaining length.  This is variable in itself */
    len += decodePacket(c, (int *)&rem_len, TimerLeftMS(timer));

    /* 3. read the rest of the buffer using a callback to supply the rest of the data */
    if (rem_len > 0 && (rc = c->ipstack->mqttread(c, c->readbuf + len, rem_len, TimerLeftMS(timer)) != rem_len)) {
        rc = 0;
        goto exit;
    }

    c->read_len = len + rem_len;
    header.byte = c->readbuf[0];
    rc = header.bits.type;
    if (c->keepAliveInterval) {
        if (c->isbroker) {
            TimerCountdown(&c->last_received, c->keepAliveInterval + c->command_timeout_ms/1000 + 1);
            DEBUG_PRINT("%d: readPacket %p(%p) deadline %d\n",
                    clock_ticks, c->pcx, c->pcx->p_hw_link, c->last_received.fbtimer.end_ticks);
        }
        else {
            TimerCountdown(&c->last_received, c->keepAliveInterval); // record the fact that we have successfully received a packet
        }
    }
exit:
    return rc;
}


// assume topic filter and name is in correct format
// # can only be at end
// + and # can only be next to separator
static char isTopicMatched(char* topicFilter, MQTTString* topicName)
{
    char* curf = topicFilter;
    char* curn = topicName->cstring ? topicName->cstring : topicName->lenstring.data;
    char* curn_end = curn + (topicName->cstring ? strlen(topicName->cstring) : (uint32_t)topicName->lenstring.len);

    while (*curf && curn < curn_end)
    {
        if (*curn == '/' && *curf != '/')
            break;
        if (*curf != '+' && *curf != '#' && *curf != *curn)
            break;
        if (*curf == '+')
        {   // skip until we meet the next separator, or end of string
            char* nextpos = curn + 1;
            while (nextpos < curn_end && *nextpos != '/')
                nextpos = ++curn + 1;
        }
        else if (*curf == '#')
            curn = curn_end - 1;    // skip until end of string
        curf++;
        curn++;
    }

    return (curn == curn_end) && (*curf == '\0');
}

static void clientHandler(MQTTClient *c, MessageData* data)
{
    (void)c;
    (void)data;
    /* do nothing */
}

int MQTTClientIsSubscribed(MQTTClient* c, MQTTString* topicName)
{
    int i;
    // we have to find the right message handler - indexed by topic
    for (i = 0; i < MAX_MESSAGE_HANDLERS; ++i)
    {
        if (c->messageHandlers[i].topicFilter != 0 &&
                (MQTTPacket_equals(topicName, (char*)c->messageHandlers[i].topicFilter) ||
                        isTopicMatched((char*)c->messageHandlers[i].topicFilter, topicName)))
        {
            if (c->messageHandlers[i].fp == clientHandler)
            {
                DEBUG_PRINT("MQTTClientIsSubscribed(%p, %s) = 1\n", c, topicName->cstring);
                return 1;
            }
        }
    }
    DEBUG_PRINT("MQTTClientIsSubscribed(%p, %s) = 0\n", c, topicName->cstring);
    return 0;
}

static int deliverMessage(MQTTClient* c, MQTTString* topicName, MQTTMessage* message)
{
    int i;
    int rc = FAILURE;

    // we have to find the right message handler - indexed by topic
    for (i = 0; i < MAX_MESSAGE_HANDLERS; ++i)
    {
        if (c->messageHandlers[i].topicFilter != 0 && (MQTTPacket_equals(topicName, (char*)c->messageHandlers[i].topicFilter) ||
                isTopicMatched((char*)c->messageHandlers[i].topicFilter, topicName)))
        {
            if (c->messageHandlers[i].fp != NULL)
            {
                MessageData md;
                NewMessageData(&md, topicName, message);
                c->messageHandlers[i].fp(c, &md);
                rc = SUCCESS;
            }
        }
    }

    if ((rc == FAILURE || c->isbroker) && c->defaultMessageHandler != NULL)
    {
        MessageData md;
        NewMessageData(&md, topicName, message);
        c->defaultMessageHandler(c, &md);
        rc = SUCCESS;
    }
    if (c->read_len > 0) {
        c->ipstack->mqttfree(c, c->read_len);
        c->read_len = 0;
    }

    return rc;
}


int keepalive(MQTTClient* c)
{
    int rc = SUCCESS;

    if (c->keepAliveInterval == 0)
        goto exit;

    if (c->isbroker) {
        if (TimerIsExpired(&c->last_received)) {
            printf("%d: mqtt: broker %p keep alive timeout\n", clock_ticks, c->pcx);
            rc = FAILURE;
            TimerCountdown(&c->last_received, c->command_timeout_ms);
        }
        return rc;
    }

    if (TimerIsExpired(&c->last_sent) || TimerIsExpired(&c->last_received))
    {
        Timer timer;
        TimerInit(&timer);
        TimerCountdownMS(&timer, 1000);
        int len = MQTTSerialize_pingreq(c->buf, c->buf_size);
        if (len > 0 && (rc = sendPacket(c, len, &timer)) == SUCCESS) {
            async_waitfor(c, PINGRESP, NULL, c->command_timeout_ms);
        }
        TimerCountdown(&c->last_received, c->command_timeout_ms);
    }

exit:
    return rc;
}


void MQTTCleanSession(MQTTClient* c)
{
    int i = 0;

    for (i = 0; i < MAX_MESSAGE_HANDLERS; ++i)
        c->messageHandlers[i].topicFilter = NULL;
}


void MQTTCloseSession(MQTTClient* c)
{
    c->async_handler.packet_type = 0;
    c->isconnected = 0;
    c->busy = 0;
    if (c->cleansession)
        MQTTCleanSession(c);
    c->ipstack->disconnect(c);
}

static int cycle(MQTTClient* c, Timer* timer)
{
    int len = 0,
        rc = SUCCESS;

    enum msgTypes packet_type = readPacket(c, timer);     /* read the socket, see what work is due */

    if (packet_type > 0 && packet_type != c->async_handler.packet_type) {
        DEBUG_PRINT("%d: mqtt: cycle: recv: %p: %s, len %d of %d\n",
                clock_ticks, c, MQTTMsgTypeNames[packet_type], c->read_len, c->readbuf_size);
    }

    switch (packet_type)
    {
        default:
            DEBUG_PRINT("mqtt: cycle error reading packet rc %d\n", packet_type);
            /* no more data to read, unrecoverable. Or read packet fails due to unexpected network error */
            rc = packet_type;
            goto exit;
        case NONE: /* timed out reading packet */
            break;
        case CONNECT:
            break;
        case CONNACK:
            break;
        case PUBACK:
            break;
        case SUBSCRIBE:
            {
                uint8_t dup;
                uint16_t msgid;
                int count;
                MQTTString topicFilters[2];
                int reqQoSs[2];

                if (MQTTDeserialize_subscribe(&dup, &msgid, 2, &count, topicFilters, reqQoSs,
                        c->readbuf, c->readbuf_size) != 1)
                    goto exit;
                if (c->isbroker)
                {
                    len = MQTTSerialize_ack(c->buf, c->buf_size, SUBACK, 0, msgid);
                    if (len <= 0)
                        rc = FAILURE;
                    else {
                        /* simple broker treats any subscribe as a subscribe to '#' */
                        rc = MQTTSetMessageHandler(c, "#", clientHandler);
                        if (rc == SUCCESS) {
                            rc = sendPacket(c, len, timer);
                            if (c->subscribe) {
                                (*c->subscribe)(c, &topicFilters[0]);
                            }
                        }
                    }
                }
            }
            break;
        case SUBACK:
            break;
        case PUBLISH:
        {
            MQTTString topicName;
            MQTTMessage msg;
            int intQoS;
            msg.payloadlen = 0; /* this is a size_t, but deserialize publish sets this as int */
            if (MQTTDeserialize_publish(&msg.dup, &intQoS, &msg.retained, &msg.id, &topicName,
               (unsigned char**)&msg.payload, (int*)&msg.payloadlen, c->readbuf, c->readbuf_size) != 1)
                goto exit;
            msg.qos = (enum QoS)intQoS;
            deliverMessage(c, &topicName, &msg);
            if (msg.qos != QOS0)
            {
                if (msg.qos == QOS1) {
                    DEBUG_PRINT("cycle PUBACK %d\n", msg.id);
                    len = MQTTSerialize_ack(c->buf, c->buf_size, PUBACK, 0, msg.id);
                }
                else if (msg.qos == QOS2) {
                    DEBUG_PRINT("cycle PUBREC %d\n", msg.id);
                    len = MQTTSerialize_ack(c->buf, c->buf_size, PUBREC, 0, msg.id);
                }
                if (len <= 0) {
                    rc = FAILURE;
                }
                else {
                    rc = sendPacket(c, len, timer);
                }
                if (rc == FAILURE)
                    goto exit; // there was a problem
            }
            break;
        }
        case PUBREC:
            break;
        case PUBREL:
        {
            unsigned short mypacketid;
            unsigned char dup, type;
            if (MQTTDeserialize_ack(&type, &dup, &mypacketid, c->readbuf, c->readbuf_size) != 1)
                rc = FAILURE;
            else if ((len = MQTTSerialize_ack(c->buf, c->buf_size,
                (packet_type == PUBREC) ? PUBREL : PUBCOMP, 0, mypacketid)) <= 0)
                rc = FAILURE;
            else if ((rc = sendPacket(c, len, timer)) != SUCCESS) // send the PUBREL packet
                rc = FAILURE; // there was a problem
            if (rc == FAILURE)
                goto exit; // there was a problem
            break;
        }

        case PUBCOMP:
            break;
        case PINGRESP:
            /* do nothing */
            break;
        case PINGREQ:
            {
                int len = MQTTSerialize_pingresp(c->buf, c->buf_size);
                if (len > 0) {
                    printf("mqtt: ping/pong\n");
                    rc = sendPacket(c, len, timer);
                }
            }
            break;
        case DISCONNECT:
            MQTTCloseSession(c);
            return 0;
    }

    if (c->async_handler.packet_type == 0 && keepalive(c) != SUCCESS) {
        //check only keepalive FAILURE status so that previous FAILURE status can be considered as FAULT
        rc = FAILURE;
    }

exit:
    if (rc == SUCCESS)
        rc = packet_type;
    else if (c->isconnected)
        MQTTCloseSession(c);
    return rc;
}


int MQTTYield(MQTTClient* c, int timeout_ms)
{
    int rc = SUCCESS;
    Timer timer;

    TimerInit(&timer);
    TimerCountdownMS(&timer, timeout_ms);

	  do
    {
        if (cycle(c, &timer) < 0)
        {
            rc = FAILURE;
            break;
        }
  	} while (!TimerIsExpired(&timer));

    return rc;
}

int MQTTIsConnected(MQTTClient* client)
{
  return client->isconnected;
}

/* Can't switch connections while this is true */
int MQTTIsBusy(MQTTClient* client)
{
  return client->isconnected && (client->async_handler.packet_type || client->read_len);
}

void MQTTRun(void* parm)
{
	Timer timer;
	MQTTClient* c = (MQTTClient*)parm;

	TimerInit(&timer);

	while (1)
	{
#if defined(MQTT_TASK)
		MutexLock(&c->mutex);
#endif
		TimerCountdownMS(&timer, 500); /* Don't wait too long if no traffic is incoming */
		cycle(c, &timer);
#if defined(MQTT_TASK)
		MutexUnlock(&c->mutex);
#endif
	}
}


#if defined(MQTT_TASK)
int MQTTStartTask(MQTTClient* client)
{
	return ThreadStart(&client->thread, &MQTTRun, client);
}
#endif


void MQTTCycle(MQTTClient* c)
{
    Timer timer;
    enum msgTypes rc;

    if (c->busy) {
        return;
    }

    TimerCountdownMS(&timer, 500); /* Don't wait too long if no traffic is incoming */
    rc = cycle(c, &timer);
    if (c->async_handler.packet_type != 0)
    {
        if (rc == c->async_handler.packet_type)
        {
            uint32_t dur;

            dur = clock_ticks - c->stats.started_tick;
            c->stats.sum += dur;
            c->stats.cnt += 1;
            if (dur < c->stats.min) {
                c->stats.min = dur;
            }
            if (dur > c->stats.max) {
                c->stats.max = dur;
            }

            DEBUG_PRINT("%d: mqtt: %p(%p) got %s\n", clock_ticks, c->pcx, c->pcx->p_hw_link, MQTTMsgTypeNames[rc]);
            if (c->async_handler.fp)
            {
                (*c->async_handler.fp)(c);
            }
            c->async_handler.packet_type = 0;
        }
        else if (TimerIsExpired(&c->async_handler.timer))
        {
            printf("%d: mqtt: MQTTCycle: %p(%p): timeout waiting for %s\n",
                   clock_ticks, c->pcx, c->pcx->p_hw_link, MQTTMsgTypeNames[c->async_handler.packet_type]);
            MQTTCloseSession(c);
        }
    }
    if (c->read_len > 0) {
        c->ipstack->mqttfree(c, c->read_len);
        c->read_len = 0;
    }
}

void async_waitfor(MQTTClient* c, int packet_type, void (*fp)(MQTTClient *), int timeout_ms)
{
    if (packet_type == CONNACK) {
        timeout_ms *= 2;
    }
    DEBUG_PRINT("%d: mqtt: %p(%p) waitfor: %s, %d ms\n",
                clock_ticks, c->pcx, c->pcx->p_hw_link, MQTTMsgTypeNames[packet_type], timeout_ms);

    c->stats.started_tick = clock_ticks;

    ASSERT(timeout_ms != 0);
    ASSERT(packet_type != 0);
    c->async_handler.fp = fp;
    c->async_handler.packet_type = packet_type;
    TimerCountdownMS(&c->async_handler.timer, timeout_ms);
}

int MQTTConnectStart(MQTTClient* c, MQTTPacket_connectData* options)
{
    Timer connect_timer;
    int rc = FAILURE;
    MQTTPacket_connectData default_options = MQTTPacket_connectData_initializer;
    int len = 0;

    DEBUG_PRINT("MQTTConnectStart\n");
#if defined(MQTT_TASK)
    MutexLock(&c->mutex);
#endif
    if (c->isconnected) /* don't send connect packet again if we are already connected */
        goto exit;

    TimerInit(&connect_timer);
    TimerCountdownMS(&connect_timer, c->command_timeout_ms);

    if (options == 0)
        options = &default_options; /* set default options if none were supplied */

    c->keepAliveInterval = options->keepAliveInterval;
    c->cleansession = options->cleansession;
    TimerCountdown(&c->last_received, c->keepAliveInterval);
    if ((len = MQTTSerialize_connect(c->buf, c->buf_size, options)) <= 0)
        goto exit;
    if ((rc = sendPacket(c, len, &connect_timer)) != SUCCESS)  // send the connect packet
        goto exit;
    else
        rc = SUCCESS;

    async_waitfor(c, CONNACK, ConnectEnd, c->command_timeout_ms);
exit:

#if defined(MQTT_TASK)
      MutexUnlock(&c->mutex);
#endif
      return rc;
}

void ConnectEnd(MQTTClient *c)
{
    MQTTConnackData data;
    if (MQTTDeserialize_connack(&data.sessionPresent, &data.rc, c->readbuf, c->readbuf_size) == 1)
    {
        if (data.rc == SUCCESS)
        {
            c->isconnected = 1;
            DEBUG_PRINT("mqtt: connected OK\n");
            return;
        }
        else if ((data.rc == 5 || data.rc == 4) && c->authentication_failed)
        {
            c->authentication_failed(c);
        }
        printf("mqtt: error: %d CONNACK\n", data.rc);
    }
    else {
        printf("mqtt: error: reading CONNACK\n");
    }
    MQTTCloseSession(c);
}

int MQTTSetMessageHandler(MQTTClient* c, const char* topicFilter, messageHandler messageHandler)
{
    int rc = FAILURE;
    int i = -1;

    DEBUG_PRINT("MQTTSetMessageHandler(%p, %s)\n", c, topicFilter);

    c->handler_index = -1;

    /* first check for an existing matching slot */
    for (i = 0; i < MAX_MESSAGE_HANDLERS; ++i)
    {
        if (c->messageHandlers[i].topicFilter != NULL && strcmp(c->messageHandlers[i].topicFilter, topicFilter) == 0)
        {
            if (messageHandler == NULL) /* remove existing */
            {
                c->messageHandlers[i].topicFilter = NULL;
                c->messageHandlers[i].fp = NULL;
            }
            rc = SUCCESS; /* return i when adding new subscription */
            break;
        }
    }
    /* if no existing, look for empty slot (unless we are removing) */
    if (messageHandler != NULL) {
        if (rc == FAILURE)
        {
            for (i = 0; i < MAX_MESSAGE_HANDLERS; ++i)
            {
                if (c->messageHandlers[i].topicFilter == NULL)
                {
                    rc = SUCCESS;
                    break;
                }
            }
        }
        if (i < MAX_MESSAGE_HANDLERS)
        {
            c->messageHandlers[i].topicFilter = topicFilter;
            c->messageHandlers[i].fp = messageHandler;
            c->handler_index = i;
        }
    }
    return rc;
}

int MQTTSubscribeStart(MQTTClient* c, const char* topicFilter, enum QoS qos, messageHandler messageHandler)
{
    int rc = FAILURE;
    Timer timer;
    int len = 0;
    MQTTString topic = MQTTString_initializer;
    topic.cstring = (char *)topicFilter;

#if defined(MQTT_TASK)
      MutexLock(&c->mutex);
#endif
      if (!c->isconnected)
            goto exit;

    TimerInit(&timer);
    TimerCountdownMS(&timer, c->command_timeout_ms);

    len = MQTTSerialize_subscribe(c->buf, c->buf_size, 0, getNextPacketId(c), 1, &topic, (int*)&qos);
    if (len <= 0)
        goto exit;
    if ((rc = sendPacket(c, len, &timer)) != SUCCESS) // send the subscribe packet
        goto exit;             // there was a problem
    if (messageHandler) {
        rc = MQTTSetMessageHandler(c, topicFilter, messageHandler);
        if (rc != SUCCESS)
            goto exit;
    }
    DEBUG_PRINT("MQTTSubscribeStart: %s\n", topicFilter);
    async_waitfor(c, SUBACK, SubscribeEnd, c->command_timeout_ms);

exit:
    if (rc == FAILURE)
        MQTTCloseSession(c);
#if defined(MQTT_TASK)
      MutexUnlock(&c->mutex);
#endif
    return rc;
}

static void SubscribeEnd(MQTTClient* c)
{
    MQTTSubackData data;
    int count = 0;
    unsigned short mypacketid;
    data.grantedQoS = QOS0;
    if (MQTTDeserialize_suback(&mypacketid, 1, &count, (int*)&data.grantedQoS, c->readbuf, c->readbuf_size) == 1)
    {
        if (data.grantedQoS == SUBFAIL && c->handler_index != -1)
        {
            DEBUG_PRINT("removing handler %s\n", c->messageHandlers[c->handler_index].topicFilter);
            c->messageHandlers[c->handler_index].topicFilter = NULL;
        }
    }
}

int MQTTPublishStart(MQTTClient* c, const char* topicName, MQTTMessage* message)
{
    int rc = FAILURE;
    Timer timer;
    MQTTString topic = MQTTString_initializer;
    topic.cstring = (char *)topicName;
    int len = 0;

#if defined(MQTT_TASK)
      MutexLock(&c->mutex);
#endif
      if (!c->isconnected)
            goto exit;

    TimerInit(&timer);
    TimerCountdownMS(&timer, c->command_timeout_ms);

    if (message->qos)
        message->id = getNextPacketId(c);

    len = MQTTSerialize_publish(c->buf, c->buf_size, 0, message->qos, message->retained, message->id,
              topic, (unsigned char*)message->payload, message->payloadlen);
    if (len <= 0)
        goto exit;

    DEBUG_PRINT("%d: mqtt: PUBLISH id %d, %s, %d, qos %d\n",
                clock_ticks, message->id, topic.cstring, message->payloadlen, message->qos);
    if ((rc = sendPacket(c, len, &timer)) != SUCCESS) // send the subscribe packet
        goto exit; // there was a problem
    if (message->qos) {
        async_waitfor(c, PUBACK, PublishEnd, c->command_timeout_ms);
    }

exit:
    if (rc == FAILURE)
        MQTTCloseSession(c);
#if defined(MQTT_TASK)
      MutexUnlock(&c->mutex);
#endif
    return rc;
}

static void PublishEnd(MQTTClient* c)
{
    unsigned short mypacketid;
    unsigned char dup, type;
    if (MQTTDeserialize_ack(&type, &dup, &mypacketid, c->readbuf, c->readbuf_size) != 1) {
        printf("mqtt: PublishEnd error reading ack\n");
        MQTTCloseSession(c);
        return;
    }
}

int MQTTDisconnect(MQTTClient* c)
{
    int rc = FAILURE;
    Timer timer;     // we might wait for incomplete incoming publishes to complete
    int len = 0;

#if defined(MQTT_TASK)
	MutexLock(&c->mutex);
#endif
    TimerInit(&timer);
    TimerCountdownMS(&timer, c->command_timeout_ms);

	  len = MQTTSerialize_disconnect(c->buf, c->buf_size);
    if (len > 0)
        rc = sendPacket(c, len, &timer);            // send the disconnect packet
    MQTTCloseSession(c);

#if defined(MQTT_TASK)
	  MutexUnlock(&c->mutex);
#endif
    return rc;
}

void MQTTServerStart(MQTTClient* c)
{
    int i;
    DEBUG_PRINT("MQTTServerStart\n");

    for (i = 0; i < MAX_MESSAGE_HANDLERS; ++i) {
        c->messageHandlers[i].topicFilter = NULL;
    }

    async_waitfor(c, CONNECT, ClientConnect, c->command_timeout_ms);
}

static void ClientConnect(MQTTClient* c)
{
    int rc;
    MQTTPacket_connectData data;
    int len = 0;
    Timer timer;

    rc = MQTTDeserialize_connect(&data, c->readbuf, c->readbuf_size);
    if (rc != 1) {
        printf("mqtt: deserialize_connect error %d\n", rc);
        goto exit;
    }

    rc = 0;

    if (c->auth && !(*c->auth)(c, &data.username, &data.password)) {
        rc = 5;
        printf("mqtt: auth failed: %d\n", rc);
    }

    TimerInit(&timer);
    TimerCountdownMS(&timer, c->command_timeout_ms);
    if ((len = MQTTSerialize_connack(c->buf, c->buf_size, rc, 0)) <= 0) {
        printf("mqtt: serialize_connack error %d\n", len);
        goto exit;
    }
    if ((rc = sendPacket(c, len, &timer)) != SUCCESS) {
        printf("mqtt: send connack error %d\n", rc);
        goto exit;
    }
    if (rc == 0) {
        c->isconnected = 1;
        c->isbroker = 1;
        c->keepAliveInterval = data.keepAliveInterval;
        TimerCountdown(&c->last_received, c->keepAliveInterval + c->command_timeout_ms/1000 + 1);
        printf("mqtt: client connected keep alive %d %p(%p)\n", c->keepAliveInterval, c->pcx, c->pcx->p_hw_link);
        return;
    }
exit:
    MQTTCloseSession(c);
    return;
}

