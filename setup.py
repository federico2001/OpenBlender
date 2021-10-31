from distutils.core import setup
setup(
  name = 'OpenBlender',         # How you named your package folder (MyLib)
  packages = ['OpenBlender'],   
  version = '2.11',      
  license='MIT',        
  description = 'OpenBlender API Service',   
  author = 'Federico Riveroll',                   
  author_email = 'federico@openblender.io',      
  url = 'https://www.openblender.io',   # Provide either the link to your github or to your website
  download_url = 'https://github.com/federico2001/OpenBlender/archive/v_2_11.tar.gz',    
  keywords = ['OpenBlender'],   # Keywords that define your package best
  install_requires=[            
          'numpy',
          'pandas',
          'datetime',
          'requests'
      ],
  classifiers=[
    'Development Status :: 3 - Alpha',      # Chose either "3 - Alpha", "4 - Beta" or "5 - Production/Stable" as the current state of your package
    'Intended Audience :: Developers',      # Define that your audience are developers
    'Topic :: Software Development :: Build Tools',
    'License :: OSI Approved :: MIT License',   # Again, pick a license
    'Programming Language :: Python :: 3',      #Specify which pyhton versions that you want to support
    'Programming Language :: Python :: 3.4',
    'Programming Language :: Python :: 3.5',
    'Programming Language :: Python :: 3.6',
    'Programming Language :: Python :: 3.7',
    'Programming Language :: Python :: 3.8',
    'Programming Language :: Python :: 3.9'
  ],
)
