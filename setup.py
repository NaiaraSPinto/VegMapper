from setuptools import setup, Extension

setup (
    name='vegmapper',
    version='1.00',
    author = 'Naiara Pinto et al.',
    packages = [
        'vegmapper',
        'vegmapper.alos2',
        'vegmapper.asf',
        'vegmapper.core',
        'vegmapper.gee',
        'vegmapper.pathurl',
        'vegmapper.s1',
    ],
    #   package_data = {'vegmapper': ['share/*.dat']},
    #   libraries = [clib],
    #   cmdclass = {"build_ext": build_ext},
    #   ext_modules = [ext],
    scripts = [
       'vegmapper/scripts/calc_vrt_stats.py',
       'vegmapper/scripts/remove_edges.py',
    ]
)
