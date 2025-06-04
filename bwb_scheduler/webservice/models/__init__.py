from .rna_seq import OWdeseq2
from .salmon_quant import SalmonQuant

MODEL_LIST = {
    "rna_seq": OWdeseq2,
    "salmon_quant": SalmonQuant,
}


__all__ = ["MODEL_LIST"]
