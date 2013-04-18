import generator as gen
import edf_generators as edf

gen.register_generator("G-EDF", edf.GedfGenerator)
gen.register_generator("P-EDF", edf.PedfGenerator)
gen.register_generator("C-EDF", edf.CedfGenerator)
gen.register_generator("P-EDF-DGL", edf.PedfDglGenerator)
