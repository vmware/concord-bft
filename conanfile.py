import conans

class ConcordBft(conans.ConanFile):
    name = 'concord-bft'
    version = '0.1.0'
    generators = 'cmake'
    requires = ('gtest/1.8.1@bincrafters/stable','cryptopp/5.6.5@bincrafters/stable')