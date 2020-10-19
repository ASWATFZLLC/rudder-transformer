const ivm = require("isolated-vm");
const fetch = require("node-fetch");
const _ = require("lodash");
const stats = require("./stats");
const {
  getTransformationCode,
  getLibraryCode
} = require("./customTransforrmationsStore");

const isolateVmMem = 64;
async function loadModule(isolateInternal, contextInternal, moduleCode) {
  const module = await isolateInternal.compileModule(moduleCode);
  await module.instantiate(contextInternal, () => {});
  return module;
}

async function createIvm(versionId, libraryVersionIds) {
  const createIvmStartTime =  new Date()
  const transformation = await getTransformationCode(versionId);
  const libraries = await Promise.all(
    libraryVersionIds.map(async libraryVersionId =>
      getLibraryCode(libraryVersionId)
    )
  );
  const librariesMap = {};
  if (transformation && libraries) {
    // TODO: Check if this should this be &&
    libraries.forEach(library => {
      librariesMap[_.camelCase(library.name)] = library.code;
    });
  }

  let { code } = transformation;
  code = code.replace("metadata(", "metadata(eventsMetadata, ");

  const match = `function transform(events) {`;
  const replacement = `
function metadata(eventsMetadata, event) {
    log("Inside modified code");
    log(eventsMetadata);
    return eventsMetadata[event.messageId] || {};
}

export function transform(fullEvents) {
  const events = fullEvents.map(event => event.message);
  const eventsMetadata = {};
  fullEvents.forEach(ev => {
    eventsMetadata[ev.message.messageId] = ev.metadata;
  });
`;
  code = code.replace(match, replacement);
  // TODO: Decide on the right value for memory limit
  const isolate = new ivm.Isolate({ memoryLimit: isolateVmMem });
  const isolateStartWallTime = isolate.wallTime;
  const isolateStartCPUTime = isolate.cpuTime;
  const context = await isolate.createContext();

  const compiledModules = {};

  await Promise.all(
    Object.entries(librariesMap).map(async ([moduleName, moduleCode]) => {
      compiledModules[moduleName] = {
        module: await loadModule(isolate, context, moduleCode)
      };
    })
  );

  // TODO: Add rudder nodejs sdk to libraries

  const jail = context.global;

  // This make the global object available in the context as 'global'. We use 'derefInto()' here
  // because otherwise 'global' would actually be a Reference{} object in the new isolate.
  await jail.set("global", jail.derefInto());

  // The entire ivm module is transferable! We transfer the module to the new isolate so that we
  // have access to the library from within the isolate.
  await jail.set("_ivm", ivm);
  await jail.set(
    "_fetch",
    new ivm.Reference(async (resolve, ...args) => {
      try {
        const res = await fetch(...args);
        const data = await res.json();
        resolve.applyIgnored(undefined, [
          new ivm.ExternalCopy(data).copyInto()
        ]);
      } catch (error) {
        resolve.applyIgnored(undefined, [
          new ivm.ExternalCopy("ERROR").copyInto()
        ]);
      }
    })
  );

  await jail.set(
    "_log",
    new ivm.Reference((...args) => {
      console.log("Log: ", ...args);
    })
  );

  await jail.set(
    "_metadata",
    new ivm.Reference((...args) => {
      const eventMetadata = eventsMetadata[args[0].messageId] || {};
      return new ivm.ExternalCopy(eventMetadata).copyInto();
    })
  );

  const bootstrap = await isolate.compileScript(
    "new " +
      `
    function() {
      // Grab a reference to the ivm module and delete it from global scope. Now this closure is the
      // only place in the context with a reference to the module. The 'ivm' module is very powerful
      // so you should not put it in the hands of untrusted code.
      let ivm = _ivm;
      delete _ivm;

      // Now we create the other half of the 'log' function in this isolate. We'll just take every
      // argument, create an external copy of it and pass it along to the log function above.
      let fetch = _fetch;
      delete _fetch;
      global.fetch = function(...args) {
        // We use 'copyInto()' here so that on the other side we don't have to call 'copy()'. It
        // doesn't make a difference who requests the copy, the result is the same.
        // 'applyIgnored' calls 'log' asynchronously but doesn't return a promise-- it ignores the
        // return value or thrown exception from 'log'.
        return new Promise(resolve => {
          fetch.applyIgnored(undefined, [
            new ivm.Reference(resolve),
            ...args.map(arg => new ivm.ExternalCopy(arg).copyInto())
          ]);
        });
      };

      // Now we create the other half of the 'log' function in this isolate. We'll just take every
      // argument, create an external copy of it and pass it along to the log function above.
      let log = _log;
      delete _log;
      global.log = function(...args) {
        // We use 'copyInto()' here so that on the other side we don't have to call 'copy()'. It
        // doesn't make a difference who requests the copy, the result is the same.
        // 'applyIgnored' calls 'log' asynchronously but doesn't return a promise-- it ignores the
        // return value or thrown exception from 'log'.
        log.applyIgnored(
          undefined,
          args.map(arg => new ivm.ExternalCopy(arg).copyInto())
          );
        };

        // Now we create the other half of the 'metadata' function in this isolate. We'll just take every
        // argument, create an external copy of it and pass it along to metadata log function above.
        let metadata = _metadata;
        delete _metadata;
        global.metadata = function(...args) {
          // We use 'copyInto()' here so that on the other side we don't have to call 'copy()'. It
          // doesn't make a difference who requests the copy, the result is the same.
          // 'applyIgnored' calls 'metadata' asynchronously but doesn't return a promise-- it ignores the
          // return value or thrown exception from 'metadata'.
          return metadata.applySync(
            undefined,
            args.map(arg => new ivm.ExternalCopy(arg).copyInto())
          );
        };


        return new ivm.Reference(function forwardMainPromise(
          fnRef,
          resolve,
          events
          ) {
            const derefMainFunc = fnRef.deref();
            Promise.resolve(derefMainFunc(events))
            .then(value => {
              resolve.applyIgnored(undefined, [
                new ivm.ExternalCopy(value).copyInto()
              ]);
            })
            .catch(error => {
              resolve.applyIgnored(undefined, [
                new ivm.ExternalCopy(error.message).copyInto()
              ]);
            });
          });
        }

        `
  );

  // Now we can execute the script we just compiled:
  const bootstrapScriptResult = await bootstrap.run(context);
  // const customScript = await isolate.compileScript(`${library} ;\n; ${code}`);
  const customScriptModule = await isolate.compileModule(`${code}`);
  await customScriptModule.instantiate(context, spec => {
    if (librariesMap[spec]) {
      return compiledModules[spec].module;
    }
    console.log(`import from ${spec} failed. Module not found.`);
    return undefined;
  });

  // const base64Script = await isolate.compileScript(base64);
  // const customScriptRunResult = await customScript.run(context);
  // console.log(
  //   "runUserTransform -> customScriptRunResult",
  //   customScriptRunResult
  // );
  await customScriptModule.evaluate();
  // const fnRefOld = await customScriptModule.namespace.get("transform");
  // console.log("runUserTransform -> fnRefOld", fnRefOld);

  const supportedFuncNames = ["transform", "transformEvent", "transformBatch"];
  let supportedFuncs = {};

  await Promise.all(
    supportedFuncNames.map(async sName => {
      const funcRef = await customScriptModule.namespace.get(sName);
      if (funcRef) {
        supportedFuncs[sName] = funcRef;
      }
    })
  );

  if (Object.keys(supportedFuncs).length !== 1) {
      throw new Error(
      `Expected one of ${supportedFuncNames}. Found ${Object.keys(supportedFuncs)}`
    );
  }

  const fnRef = supportedFuncs[0];
  stats.timing("createivm_duration", createIvmStartTime);
  // TODO : check if we can resolve this
  // eslint-disable-next-line no-async-promise-executor
  return {
    isolate,
    jail,
    bootstrapScriptResult,
    context,
    fnRef,
    isolateStartWallTime,
    isolateStartCPUTime
  };
}



async function getFactory(versionId, libraryVersionIds) {
  const factory = {
    create: async () => {
      return createIvm(versionId, libraryVersionIds);
    },
    destroy: async client => {
      await client.isolate.dispose();
    }
  };

  return factory;
}

exports.getFactory = getFactory;