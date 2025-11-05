import json
import os
from datetime import datetime

class Storage:
    def __init__(self, data_dir="/app/data"):
        self.data_dir = data_dir
        os.makedirs(data_dir, exist_ok=True)
        
        self.users_file = os.path.join(data_dir, "users.json")
        self.channels_file = os.path.join(data_dir, "channels.json")
        self.messages_file = os.path.join(data_dir, "messages.json")
        self.private_messages_file = os.path.join(data_dir, "private_messages.json")
        
        for file in [self.users_file, self.channels_file, self.messages_file, self.private_messages_file]:
            if not os.path.exists(file):
                with open(file, 'w') as f:
                    json.dump([], f)
    
    def _read_file(self, filename):
        try:
            with open(filename, 'r') as f:
                return json.load(f)
        except:
            return []
    
    def _write_file(self, filename, data):
        with open(filename, 'w') as f:
            json.dump(data, f, indent=2)
    
    def save_user(self, username, timestamp):
        users = self._read_file(self.users_file)
        users.append({"username": username, "timestamp": timestamp})
        self._write_file(self.users_file, users)
    
    def get_users(self):
        users = self._read_file(self.users_file)
        return [u["username"] for u in users]
    
    def user_exists(self, username):
        return username in self.get_users()
    
    def save_channel(self, channel_name):
        channels = self._read_file(self.channels_file)
        channels.append({"name": channel_name, "created_at": datetime.now().isoformat()})
        self._write_file(self.channels_file, channels)
    
    def get_channels(self):
        channels = self._read_file(self.channels_file)
        return [c["name"] for c in channels]
    
    def channel_exists(self, channel_name):
        return channel_name in self.get_channels()
    
    def save_message(self, message_data):
        messages = self._read_file(self.messages_file)
        messages.append(message_data)
        self._write_file(self.messages_file, messages)
    
    def save_private_message(self, message_data):
        messages = self._read_file(self.private_messages_file)
        messages.append(message_data)
        self._write_file(self.private_messages_file, messages)