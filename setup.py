from setuptools import setup, find_packages


setup(name='mroylib_min',
    version='2.0.7.3',
    description='some libs',
    url='https://github.com/Qingluan/.git',
    author='Qing luan',
    author_email='darkhackdevil@gmail.com',
    license='MIT',
    include_package_data=True,
    zip_safe=False,
    packages=find_packages(),
    install_requires=['tornado','motor','tabulate', 'redis', 'pymysql','pymongo','tqdm', 'xlrd','xlwt','bs4','requests','termcolor','simplejson','pysocks','telethon'],
    entry_points={
        'console_scripts' : [
            'Mr=mroylib.cmd:main',
            'repo-upload=mroylib.api:repo_upload_client',
            'm-server=mroylib.servers.tornado:main',
        ]
    }
)


