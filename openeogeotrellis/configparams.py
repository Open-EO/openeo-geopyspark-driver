import os

class ConfigParams:

    def __init__(self):
        self.zookeepernodes=os.environ.get("ZOOKEEPERNODES",
            'epod-master1.vgt.vito.be:2181,epod-master2.vgt.vito.be:2181,epod-master3.vgt.vito.be:2181'    
        ).split(',') 
        self.register_layers_to_accumulo=os.environ.get("REGISTER_LAYERS_TO_ACCUMULO","True").lower() in ('true','yes','1')
    
    
    
