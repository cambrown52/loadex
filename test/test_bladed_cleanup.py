from pathlib import Path

from loadex.formats.bladed_out_file import BladedOutFile
import dnv_bladed_results as bd


current_directory=Path(__file__).parent
data_directory = current_directory / "data" / "Bladed"


def test_bladed_unload():
    filepath=Path(data_directory / "idling.$PJ")
    run=bd.ResultsApi.get_run(str(filepath.parent),filepath.stem)
    

    # try a 1D variable
    variable_name='Rotating hub Mx'
    run.get_variable_1d(variable_name).get_data()
    run.unload_variable(variable_name)

    # try a 2D variable, 1nd independent variable
    variable_name="Tower Mx"
    independent_variable_values=run.get_variable_2d("Tower Mx").get_independent_variable(bd.INDEPENDENT_VARIABLE_ID_SECONDARY).get_values_as_string()
    run.get_variable_2d(variable_name).get_data_at_value(independent_variable_values[0])
    run.unload_variable(variable_name)

    # try a 2D variable, 2nd independent variable
    variable_names=["Tower Mx","Tower My", "Tower Mz", "Tower Myz", "Tower Fx", "Tower Fy", "Tower Fz", "Tower Fyz"]   
    for name in variable_names:
        variable=run.get_variable_2d_from_specific_group(name,"Tower member loads - local coordinates")
        independent_variable_values=variable.get_independent_variable(bd.INDEPENDENT_VARIABLE_ID_SECONDARY).get_values_as_string()
        for value in independent_variable_values:
            variable.get_data_at_value(value)
            run.unload_variable_from_specific_group(variable.name,variable.parent_group_name)
    
    



def test_load_delete_full_file():

    f=BladedOutFile(str(data_directory / "idling.$PJ"))
    with open("test_output.txt","w") as out_file:
        out_file.write(f"Loaded file: {f.filepath}\n")
    for s in f.sensors:
        with open("test_output.txt","a") as out_file:
            out_file.write(f"run.get_variable('{s.variable_name}','{s.group_name}')\n")
            out_file.write(f"run.unload_variable_from_specific_group('{s.variable_name}','{s.group_name}')\n")
        s.get_data()
        #f.run.unload_variable_from_specific_group(s.variable_name,s.group_name)
    
    del f


def test_load_delete_partial_file():

    f=BladedOutFile(str(data_directory / "idling.$PJ"))
    f.sensors[0].get_data()
    f.sensors[10].get_data()
    f.sensors[100].get_data()
    f.sensors[-1].get_data()
    
    del f