module.exports = (config) => {
  config.set({
    browsers: ['Chrome'],
    frameworks: ['jasmine'],
    singleRun: true,

    files: ['app/**/*test.js'],
    preprocessors: {
      'app/**/*.js': ['webpack']
    },

    webpack: {
      devtool: 'cheap-module-eval-source-map',
      module: {
        loaders: [
          {pattern: /\.jsx?$/, loader: 'babel', exclude: /node_modules/},
          {test: /\.css$/, loader: 'style!css?module&localIdentName=[name]__[local]'}
        ]
      }
    }
  })
}
