#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include "modusdb_wrapper.hpp"

namespace py = pybind11;

PYBIND11_MODULE(modusdb, m) {
    py::class_<Engine>(m, "Engine")
        .def(py::init<const std::string&>())
        .def("create_namespace", &Engine::createNamespace)
        .def("get_namespace", &Engine::getNamespace)
        .def("drop_all", &Engine::dropAll)
        .def("load", &Engine::load)
        .def("load_data", &Engine::loadData)
        .def("close", &Engine::close);

    py::class_<Namespace>(m, "Namespace")
        .def("get_id", &Namespace::getId)
        .def("drop_data", &Namespace::dropData)
        .def("alter_schema", &Namespace::alterSchema)
        .def("mutate", &Namespace::mutate)
        .def("query", &Namespace::query);
}