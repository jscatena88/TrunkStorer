import os
import json
import base64
import paho.mqtt.client as mqtt
from sqlalchemy import (
    create_engine, Column, Integer, String, Float, Boolean, ForeignKey, LargeBinary, JSON
)
from sqlalchemy.orm import relationship, sessionmaker, declarative_base

# Define the base class for declarative models
Base = declarative_base()

# Define the System model
class System(Base):
    __tablename__ = 'systems'
    sys_num = Column(Integer, primary_key=True)
    sys_name = Column(String)
    type = Column(String)
    sysid = Column(String)
    wacn = Column(String)
    nac = Column(String)
    rfss = Column(Integer)
    site_id = Column(Integer)

    units = relationship('Unit', back_populates='system')
    talkgroups = relationship('Talkgroup', back_populates='system')
    calls = relationship('Call', back_populates='system')
    messages = relationship('Message', back_populates='system')

# Define the Unit model
class Unit(Base):
    __tablename__ = 'units'
    unit_id = Column(Integer, primary_key=True)
    unit_alpha_tag = Column(String)
    sys_num = Column(Integer, ForeignKey('systems.sys_num'))

    system = relationship('System', back_populates='units')
    calls = relationship('Call', back_populates='unit')

# Define the Talkgroup model
class Talkgroup(Base):
    __tablename__ = 'talkgroups'
    talkgroup_id = Column(Integer, primary_key=True)
    talkgroup_alpha_tag = Column(String)
    talkgroup_description = Column(String)
    talkgroup_group = Column(String)
    talkgroup_tag = Column(String)
    talkgroup_patches = Column(String)
    sys_num = Column(Integer, ForeignKey('systems.sys_num'))

    system = relationship('System', back_populates='talkgroups')
    calls = relationship('Call', back_populates='talkgroup')

# Define the Recorder model
class Recorder(Base):
    __tablename__ = 'recorders'
    id = Column(String, primary_key=True)
    src_num = Column(Integer)
    rec_num = Column(Integer)
    type = Column(String)
    freq = Column(Integer)
    duration = Column(Float)
    count = Column(Integer)
    rec_state = Column(Integer)
    rec_state_type = Column(String)
    squelched = Column(Boolean)

    calls = relationship('Call', back_populates='recorder')

# Define the Call model
class Call(Base):
    __tablename__ = 'calls'
    id = Column(String, primary_key=True)
    call_num = Column(Integer)
    sys_num = Column(Integer, ForeignKey('systems.sys_num'))
    freq = Column(Integer)
    unit_id = Column(Integer, ForeignKey('units.unit_id'))
    talkgroup_id = Column(Integer, ForeignKey('talkgroups.talkgroup_id'))
    elapsed = Column(Float)
    length = Column(Float)
    call_state = Column(Integer)
    call_state_type = Column(String)
    mon_state = Column(Integer)
    mon_state_type = Column(String)
    audio_type = Column(String)
    phase2_tdma = Column(Boolean)
    tdma_slot = Column(Integer)
    analog = Column(Boolean)
    rec_num = Column(Integer)
    src_num = Column(Integer)
    rec_state = Column(Integer)
    rec_state_type = Column(String)
    conventional = Column(Boolean)
    encrypted = Column(Boolean)
    emergency = Column(Boolean)
    start_time = Column(Integer)
    stop_time = Column(Integer)
    process_call_time = Column(Integer)
    error_count = Column(Integer)
    spike_count = Column(Integer)
    retry_attempt = Column(Integer)
    freq_error = Column(Integer)
    signal = Column(Integer)
    noise = Column(Integer)
    call_filename = Column(String)

    system = relationship('System', back_populates='calls')
    unit = relationship('Unit', back_populates='calls')
    talkgroup = relationship('Talkgroup', back_populates='calls')
    recorder = relationship('Recorder', back_populates='calls')
    audio = relationship('Audio', back_populates='call', uselist=False)

# Define the Audio model
class Audio(Base):
    __tablename__ = 'audio'
    call_id = Column(String, ForeignKey('calls.id'), primary_key=True)
    audio_m4a = Column(LargeBinary)
    audio_metadata = Column(JSON)

    call = relationship('Call', back_populates='audio')

# Define the Message model
class Message(Base):
    __tablename__ = 'messages'
    id = Column(Integer, primary_key=True, autoincrement=True)
    sys_num = Column(Integer, ForeignKey('systems.sys_num'))
    trunk_msg = Column(Integer)
    trunk_msg_type = Column(String)
    opcode = Column(String)
    opcode_type = Column(String)
    opcode_desc = Column(String)
    meta = Column(String)
    timestamp = Column(Integer)
    instance_id = Column(String)

    system = relationship('System', back_populates='messages')

# Create the database engine and session
# Read the database URI from environment variable
database_uri = os.environ.get('DATABASE_URI', 'sqlite:///trunk_recorder.db')
engine = create_engine(database_uri)
Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)

# MQTT message handler functions
def on_connect(client, userdata, flags, rc):
    print("Connected with result code " + str(rc))
    # Subscribe to all relevant topics
    client.subscribe([
        ("topic/#", 0),
        ("unit_topic/#", 0),
        ("message_topic/#", 0),
    ])

def on_message(client, userdata, msg):
    topic = msg.topic
    payload = msg.payload.decode()
    try:
        data = json.loads(payload)
        handle_message(topic, data)
    except Exception as e:
        print(f"Error processing message on topic {topic}: {e}")

def handle_message(topic, data):
    msg_type = data.get('type')
    timestamp = data.get('timestamp')
    instance_id = data.get('instance_id')
    session = Session()

    if msg_type == 'system':
        # Process system message
        system_data = data.get('system')
        sys_num = system_data.get('sys_num')
        sys_name = system_data.get('sys_name')

        system = session.query(System).filter_by(sys_num=sys_num).first()
        if not system:
            system = System(sys_num=sys_num)

        system.sys_name = sys_name
        system.type = system_data.get('type')
        system.sysid = system_data.get('sysid')
        system.wacn = system_data.get('wacn')
        system.nac = system_data.get('nac')
        system.rfss = system_data.get('rfss')
        system.site_id = system_data.get('site_id')

        session.add(system)
        session.commit()

    elif msg_type in ['call_start', 'call_end']:
        call_data = data.get('call')
        call_id = call_data.get('id')
        call = session.query(Call).filter_by(id=call_id).first()
        if not call:
            call = Call(id=call_id)

        call.call_num = call_data.get('call_num')
        call.sys_num = call_data.get('sys_num')
        call.freq = call_data.get('freq')
        call.elapsed = call_data.get('elapsed')
        call.length = call_data.get('length')
        call.call_state = call_data.get('call_state')
        call.call_state_type = call_data.get('call_state_type')
        call.mon_state = call_data.get('mon_state')
        call.mon_state_type = call_data.get('mon_state_type')
        call.audio_type = call_data.get('audio_type')
        call.phase2_tdma = call_data.get('phase2_tdma')
        call.tdma_slot = call_data.get('tdma_slot')
        call.analog = call_data.get('analog')
        call.rec_num = call_data.get('rec_num')
        call.src_num = call_data.get('src_num')
        call.rec_state = call_data.get('rec_state')
        call.rec_state_type = call_data.get('rec_state_type')
        call.conventional = call_data.get('conventional')
        call.encrypted = call_data.get('encrypted')
        call.emergency = call_data.get('emergency')
        call.start_time = call_data.get('start_time')
        call.stop_time = call_data.get('stop_time')
        call.process_call_time = call_data.get('process_call_time')
        call.error_count = call_data.get('error_count')
        call.spike_count = call_data.get('spike_count')
        call.retry_attempt = call_data.get('retry_attempt')
        call.freq_error = call_data.get('freq_error')
        call.signal = call_data.get('signal')
        call.noise = call_data.get('noise')
        call.call_filename = call_data.get('call_filename')

        # Get or create system
        system = session.query(System).filter_by(sys_num=call.sys_num).first()
        if not system:
            system = System(sys_num=call.sys_num, sys_name=call_data.get('sys_name'))
            session.add(system)
            session.commit()
        call.system = system

        # Get or create unit
        unit_id = call_data.get('unit')
        if unit_id is not None:
            unit = session.query(Unit).filter_by(unit_id=unit_id).first()
            if not unit:
                unit = Unit(unit_id=unit_id)
            unit.unit_alpha_tag = call_data.get('unit_alpha_tag')
            unit.sys_num = call.sys_num
            session.add(unit)
            session.commit()
            call.unit = unit

        # Get or create talkgroup
        talkgroup_id = call_data.get('talkgroup')
        if talkgroup_id is not None:
            talkgroup = session.query(Talkgroup).filter_by(talkgroup_id=talkgroup_id).first()
            if not talkgroup:
                talkgroup = Talkgroup(talkgroup_id=talkgroup_id)
            talkgroup.talkgroup_alpha_tag = call_data.get('talkgroup_alpha_tag')
            talkgroup.talkgroup_description = call_data.get('talkgroup_description')
            talkgroup.talkgroup_group = call_data.get('talkgroup_group')
            talkgroup.talkgroup_tag = call_data.get('talkgroup_tag')
            talkgroup.talkgroup_patches = call_data.get('talkgroup_patches')
            talkgroup.sys_num = call.sys_num
            session.add(talkgroup)
            session.commit()
            call.talkgroup = talkgroup

        session.add(call)
        session.commit()

    elif msg_type == 'audio':
        # Process audio message
        call_data = data.get('call')
        audio_m4a_base64 = call_data.get('audio_m4a_base64')
        metadata = call_data.get('metadata')
        call_filename = metadata.get('call_filename')
        # Extract call ID from filename or metadata
        call_id = call_filename.split('-')[0] + '_' + str(metadata.get('start_time'))
        audio = session.query(Audio).filter_by(call_id=call_id).first()
        if not audio:
            audio = Audio(call_id=call_id)
        if audio_m4a_base64:
            audio.audio_m4a = base64.b64decode(audio_m4a_base64)
        audio.audio_metadata = metadata
        session.add(audio)
        session.commit()

    elif msg_type == 'message':
        # Process trunking message
        message_data = data.get('message')
        message = Message()
        message.sys_num = message_data.get('sys_num')
        message.trunk_msg = message_data.get('trunk_msg')
        message.trunk_msg_type = message_data.get('trunk_msg_type')
        message.opcode = message_data.get('opcode')
        message.opcode_type = message_data.get('opcode_type')
        message.opcode_desc = message_data.get('opcode_desc')
        message.meta = message_data.get('meta')
        message.timestamp = data.get('timestamp')
        message.instance_id = data.get('instance_id')

        # Get or create system
        system = session.query(System).filter_by(sys_num=message.sys_num).first()
        if not system:
            system = System(sys_num=message.sys_num, sys_name=message_data.get('sys_name'))
            session.add(system)
            session.commit()
        message.system = system

        session.add(message)
        session.commit()

    # Handle other message types like 'unit' messages
    elif msg_type in ['join', 'on', 'off', 'data', 'location', 'ackresp']:
        unit_data = data.get(msg_type)
        unit_id = unit_data.get('unit')
        unit = session.query(Unit).filter_by(unit_id=unit_id).first()
        if not unit:
            unit = Unit(unit_id=unit_id)
        unit.unit_alpha_tag = unit_data.get('unit_alpha_tag')
        unit.sys_num = unit_data.get('sys_num')
        session.add(unit)
        session.commit()

        # Handle talkgroup if present
        talkgroup_id = unit_data.get('talkgroup')
        if talkgroup_id is not None:
            talkgroup = session.query(Talkgroup).filter_by(talkgroup_id=talkgroup_id).first()
            if not talkgroup:
                talkgroup = Talkgroup(talkgroup_id=talkgroup_id)
            talkgroup.talkgroup_alpha_tag = unit_data.get('talkgroup_alpha_tag')
            talkgroup.talkgroup_description = unit_data.get('talkgroup_description')
            talkgroup.talkgroup_group = unit_data.get('talkgroup_group')
            talkgroup.talkgroup_tag = unit_data.get('talkgroup_tag')
            talkgroup.talkgroup_patches = unit_data.get('talkgroup_patches')
            talkgroup.sys_num = unit.sys_num
            session.add(talkgroup)
            session.commit()

    session.close()

# Set up the MQTT client
client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1)

# Read MQTT broker details from environment variables
mqtt_broker = os.environ.get('MQTT_BROKER', 'localhost')
mqtt_port = int(os.environ.get('MQTT_PORT', 1883))
mqtt_username = os.environ.get('MQTT_USERNAME')
mqtt_password = os.environ.get('MQTT_PASSWORD')

if mqtt_username and mqtt_password:
    client.username_pw_set(mqtt_username, mqtt_password)

client.on_connect = on_connect
client.on_message = on_message

client.connect(mqtt_broker, mqtt_port, 60)

# Start the MQTT client loop
client.loop_forever()
