from setuptools import setup, Extension

setup (
    name='vegmapper',
    version='1.00',
    author = 'Naiara Pinto et al.',
    packages = [
        'vegmapper',
        'vegmapper.core',
        'vegmapper.s1',
        'vegmapper.alos2',
        'vegmapper.gee'
    ],
    #   package_data = {'vegmapper': ['share/*.dat']},
    #   libraries = [clib],
    #   cmdclass = {"build_ext": build_ext},
    #   ext_modules = [ext],
    #scripts = [
    #    'data-prep/s1_build_vrt.py',
    #    'data-prep/calc_vrt_stats.py',
    #    'data-prep/s1_remove_edges.py',
    #]
)
