#include <zmq.hpp>
#include <iostream>
#include <string>
#include <vector>
#include <random>
#include <thread>
#include <chrono>
#include <sstream>
#include <iomanip>
#include <cstring>

std::string generate_uuid() {
    static std::random_device rd;
    static std::mt19937 gen(rd());
    static std::uniform_int_distribution<> dis(0, 15);
    const char* hex = "0123456789abcdef";
    std::string uuid = "";
    for (int i = 0; i < 6; i++) {
        uuid += hex[dis(gen)];
    }
    return uuid;
}

std::string get_timestamp() {
    auto now = std::chrono::system_clock::now();
    auto time = std::chrono::system_clock::to_time_t(now);
    std::stringstream ss;
    ss << std::put_time(std::gmtime(&time), "%Y-%m-%dT%H:%M:%S") << "Z";
    return ss.str();
}

class BotCpp {
private:
    zmq::context_t context;
    zmq::socket_t req_socket;
    zmq::socket_t sub_socket;
    std::string username;
    int logical_clock;
    std::vector<std::string> messages;
    std::mt19937 gen;

    void increment_clock() {
        logical_clock++;
    }

    std::string build_message(const std::string& service, const std::string& extra_fields) {
        increment_clock();
        std::stringstream ss;
        ss << "{\"service\":\"" << service << "\",\"data\":{";
        ss << extra_fields;
        ss << ",\"timestamp\":\"" << get_timestamp() << "\"";
        ss << ",\"clock\":" << logical_clock << "}}";
        return ss.str();
    }

    std::string send_request(const std::string& msg_json) {
        zmq::message_t request(msg_json.begin(), msg_json.end());
        req_socket.send(request, zmq::send_flags::none);
        
        zmq::message_t reply;
        req_socket.recv(reply, zmq::recv_flags::none);
        
        return std::string(static_cast<char*>(reply.data()), reply.size());
    }

public:
    BotCpp() : context(1), 
               req_socket(context, zmq::socket_type::req),
               sub_socket(context, zmq::socket_type::sub),
               logical_clock(0) {
        
        std::random_device rd;
        gen = std::mt19937(rd());
        
        req_socket.connect("tcp://broker:5555");
        sub_socket.connect("tcp://proxy:5558");
        
        username = "bot_cpp_" + generate_uuid();
        
        messages = {
            "Olá do C++! 🤖",
            "Bot C++ ativo!",
            "C++ funcionando! ⚙️",
            "Mensagem via C++",
            "Hello from C++!",
            "Bot multicultural! 🌍"
        };
    }

    bool login() {
        std::cout << "[" << username << "] Login via C++..." << std::endl;
        
        std::string msg = build_message("login", "\"user\":\"" + username + "\"");
        std::string response = send_request(msg);
        
        if (response.find("sucesso") != std::string::npos) {
            sub_socket.set(zmq::sockopt::subscribe, username);
            std::cout << "[" << username << "] Conectado!" << std::endl;
            return true;
        }
        
        return false;
    }

    void create_channels() {
        std::vector<std::string> channels = {"geral", "testes", "cpp"};
        
        for (const auto& ch : channels) {
            try {
                std::string msg = build_message("channel", "\"channel\":\"" + ch + "\"");
                send_request(msg);
            } catch (...) {}
        }
    }

    bool publish(const std::string& channel, const std::string& message) {
        try {
            std::stringstream ss;
            ss << "\"user\":\"" << username << "\"";
            ss << ",\"channel\":\"" << channel << "\"";
            ss << ",\"message\":\"" << message << "\"";
            
            std::string msg = build_message("publish", ss.str());
            std::string response = send_request(msg);
            
            return response.find("OK") != std::string::npos;
        } catch (...) {
            return false;
        }
    }

    void run() {
        if (!login()) {
            return;
        }

        std::this_thread::sleep_for(std::chrono::seconds(2));
        create_channels();
        std::this_thread::sleep_for(std::chrono::seconds(1));

        std::vector<std::string> channels = {"geral", "testes", "cpp"};
        std::cout << "[" << username << "] Bot C++ iniciado!" << std::endl;

        std::uniform_int_distribution<> channel_dist(0, channels.size() - 1);
        std::uniform_int_distribution<> message_dist(0, messages.size() - 1);
        std::uniform_int_distribution<> sleep_dist(500, 2000);

        int count = 0;

        while (true) {
            try {
                std::string channel = channels[channel_dist(gen)];

                for (int i = 0; i < 10; i++) {
                    std::string message = messages[message_dist(gen)];

                    if (publish(channel, message)) {
                        count++;
                        std::cout << "[" << username << "] #" << count 
                                  << " → " << channel << ": " << message << std::endl;
                    }

                    std::this_thread::sleep_for(std::chrono::milliseconds(sleep_dist(gen)));
                }

                std::this_thread::sleep_for(std::chrono::seconds(3));

            } catch (...) {
                std::this_thread::sleep_for(std::chrono::seconds(2));
            }
        }
    }
};

int main() {
    try {
        std::cout << "Iniciando Bot C++..." << std::endl;
        std::this_thread::sleep_for(std::chrono::seconds(5));
        
        BotCpp bot;
        bot.run();
        
    } catch (const std::exception& e) {
        std::cerr << "Erro: " << e.what() << std::endl;
        return 1;
    }
    
    return 0;
}