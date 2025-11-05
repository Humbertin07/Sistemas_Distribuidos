import zmq

def main():
    context = zmq.Context()
    
    xsub = context.socket(zmq.XSUB)
    xsub.bind("tcp://*:5557")
    
    xpub = context.socket(zmq.XPUB)
    xpub.bind("tcp://*:5558")
    
    print("[PROXY] Iniciado nas portas 5557 e 5558")
    
    zmq.proxy(xsub, xpub)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n[PROXY] Encerrando...")